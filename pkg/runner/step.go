package runner

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/nektos/act/pkg/common"
	"github.com/nektos/act/pkg/container"
	"github.com/nektos/act/pkg/exprparser"
	"github.com/nektos/act/pkg/model"
	"github.com/sirupsen/logrus"
)

// ========================
// Types & Constants
// ========================

type step interface {
	pre() common.Executor
	main() common.Executor
	post() common.Executor

	getRunContext() *RunContext
	getGithubContext(ctx context.Context) *model.GithubContext
	getStepModel() *model.Step
	getEnv() *map[string]string
	getIfExpression(context context.Context, stage stepStage) string
}

type stepStage int

const (
	stepStagePre stepStage = iota
	stepStageMain
	stepStagePost
)

const maxSymlinkDepth = 10 // Symlink resolution depth for Actions

func (s stepStage) String() string {
	switch s {
	case stepStagePre:
		return "Pre"
	case stepStageMain:
		return "Main"
	case stepStagePost:
		return "Post"
	}
	return "Unknown"
}

// ========================
// Core Step Execution
// ========================

func runStepExecutor(step step, stage stepStage, executor common.Executor) common.Executor {
	return func(ctx context.Context) error {
		logger := common.Logger(ctx)
		rc := step.getRunContext()
		stepModel := step.getStepModel()

		// Initialize step result (for main steps only)
		stepResult := &model.StepResult{
			Outcome:    model.StepStatusSuccess,
			Conclusion: model.StepStatusSuccess,
			Outputs:    map[string]string{},
		}
		if stage == stepStageMain {
			rc.StepResults[stepModel.ID] = stepResult
		}
		rc.CurrentStep = stepModel.ID

		if err := setupEnv(ctx, step); err != nil {
			return err
		}

		ifExpression := step.getIfExpression(ctx, stage)
		runStep, err := isStepEnabled(ctx, ifExpression, step, stage)
		if err != nil {
			markStepFailed(stepResult)
			return err
		}

		if !runStep {
			markStepSkipped(stepResult)
			logger.WithField("stepResult", stepResult.Outcome).Debugf("Skipping step '%s' due to '%s'", stepModel, ifExpression)
			return nil
		}

		stepString := rc.ExprEval.Interpolate(ctx, stepModel.String())
		if strings.Contains(stepString, "::add-mask::") {
			stepString = "add-mask command"
		}
		logger.Infof("⭐ Run %s %s", stage, stepString)

		if err := injectGithubFileCommandsEnv(ctx, step); err != nil {
			return err
		}

		timeoutCtx, cancel := evaluateStepTimeout(ctx, rc.ExprEval, stepModel)
		defer cancel()

		start := time.Now()
		err = executor(timeoutCtx)
		duration := time.Since(start)

		if err == nil {
			logger.WithFields(logrus.Fields{"executionTime": duration, "stepResult": stepResult.Outcome}).
				Infof("✅  Success - %s %s [%s]", stage, stepString, duration)
		} else {
			stepResult.Outcome = model.StepStatusFailure
			continueOnError, parseErr := isContinueOnError(ctx, stepModel.RawContinueOnError, step, stage)
			if parseErr != nil {
				stepResult.Conclusion = model.StepStatusFailure
				return parseErr
			}
			if continueOnError {
				logger.Infof("Failed but continuing to next step")
				err = nil
				stepResult.Conclusion = model.StepStatusSuccess
			} else {
				stepResult.Conclusion = model.StepStatusFailure
			}
			logger.WithFields(logrus.Fields{"executionTime": duration, "stepResult": stepResult.Outcome}).
				Infof("❌  Failure - %s %s [%s]", stage, stepString, duration)
		}

		// Process runner command outputs
		if err := processAllRunnerFiles(ctx, rc); err != nil {
			return err
		}

		return err
	}
}

func markStepSkipped(sr *model.StepResult) {
	sr.Conclusion = model.StepStatusSkipped
	sr.Outcome = model.StepStatusSkipped
}

func markStepFailed(sr *model.StepResult) {
	sr.Conclusion = model.StepStatusFailure
	sr.Outcome = model.StepStatusFailure
}

func injectGithubFileCommandsEnv(ctx context.Context, step step) error {
	rc := step.getRunContext()
	actPath := rc.JobContainer.GetActPath()
	env := *step.getEnv()

	files := map[string]string{
		"GITHUB_OUTPUT":       path.Join("workflow", "outputcmd.txt"),
		"GITHUB_STATE":        path.Join("workflow", "statecmd.txt"),
		"GITHUB_PATH":         path.Join("workflow", "pathcmd.txt"),
		"GITHUB_ENV":          path.Join("workflow", "envs.txt"),
		"GITHUB_STEP_SUMMARY": path.Join("workflow", "SUMMARY.md"),
	}

	var entries []*container.FileEntry
	for key, file := range files {
		env[key] = path.Join(actPath, file)
		entries = append(entries, &container.FileEntry{Name: file, Mode: 0666})
	}

	return rc.JobContainer.Copy(actPath, entries...)(ctx)
}

func processAllRunnerFiles(ctx context.Context, rc *RunContext) error {
	if err := processRunnerEnvFileCommand(ctx, "workflow/envs.txt", rc, rc.setEnv); err != nil {
		return err
	}
	if err := processRunnerEnvFileCommand(ctx, "workflow/statecmd.txt", rc, rc.saveState); err != nil {
		return err
	}
	if err := processRunnerEnvFileCommand(ctx, "workflow/outputcmd.txt", rc, rc.setOutput); err != nil {
		return err
	}
	return rc.UpdateExtraPath(ctx, path.Join(rc.JobContainer.GetActPath(), "workflow/pathcmd.txt"))
}

func processRunnerEnvFileCommand(ctx context.Context, fileName string, rc *RunContext, setter func(context.Context, map[string]string, string)) error {
	env := map[string]string{}
	if err := rc.JobContainer.UpdateFromEnv(path.Join(rc.JobContainer.GetActPath(), fileName), &env)(ctx); err != nil {
		return err
	}
	for k, v := range env {
		setter(ctx, map[string]string{"name": k}, v)
	}
	return nil
}

// ========================
// Helpers & Utilities
// ========================

func evaluateStepTimeout(ctx context.Context, exprEval ExpressionEvaluator, stepModel *model.Step) (context.Context, context.CancelFunc) {
	timeout := exprEval.Interpolate(ctx, stepModel.TimeoutMinutes)
	if timeout != "" {
		if mins, err := strconv.ParseInt(timeout, 10, 64); err == nil {
			return context.WithTimeout(ctx, time.Duration(mins)*time.Minute)
		}
	}
	return ctx, func() {}
}

func setupEnv(ctx context.Context, step step) error {
	rc := step.getRunContext()

	mergeEnv(ctx, step)

	// Merge step-specific environment last to avoid overwriting
	mergeIntoMap(step, step.getEnv(), step.getStepModel().GetEnv())

	exprEval := rc.NewExpressionEvaluator(ctx)

	for k, v := range *step.getEnv() {
		if !strings.HasPrefix(k, "INPUT_") {
			(*step.getEnv())[k] = exprEval.Interpolate(ctx, v)
		}
	}

	exprEval = rc.NewExpressionEvaluatorWithEnv(ctx, *step.getEnv())
	for k, v := range *step.getEnv() {
		if strings.HasPrefix(k, "INPUT_") {
			(*step.getEnv())[k] = exprEval.Interpolate(ctx, v)
		}
	}

	common.Logger(ctx).Debugf("setupEnv => %v", *step.getEnv())
	return nil
}

func mergeEnv(ctx context.Context, step step) {
	env := step.getEnv()
	rc := step.getRunContext()
	job := rc.Run.Job()

	if c := job.Container(); c != nil {
		mergeIntoMap(step, env, rc.GetEnv(), c.Env)
	} else {
		mergeIntoMap(step, env, rc.GetEnv())
	}

	rc.withGithubEnv(ctx, step.getGithubContext(ctx), *env)

	if step.getStepModel().Uses != "" {
		// Strip INPUT_ values to prevent leaking undefined inputs into actions
		for key := range *env {
			if strings.Contains(key, "INPUT_") {
				delete(*env, key)
			}
		}
	}
}

func isStepEnabled(ctx context.Context, expr string, step step, stage stepStage) (bool, error) {
	rc := step.getRunContext()
	defaultCheck := exprparser.DefaultStatusCheckSuccess
	if stage == stepStagePost {
		defaultCheck = exprparser.DefaultStatusCheckAlways
	}
	return EvalBool(ctx, rc.NewStepExpressionEvaluatorExt(ctx, step, stage == stepStageMain), expr, defaultCheck)
}

func isContinueOnError(ctx context.Context, expr string, step step, _ stepStage) (bool, error) {
	if strings.TrimSpace(expr) == "" {
		return false, nil
	}
	rc := step.getRunContext()
	return EvalBool(ctx, rc.NewStepExpressionEvaluator(ctx, step), expr, exprparser.DefaultStatusCheckNone)
}

func mergeIntoMap(step step, target *map[string]string, maps ...map[string]string) {
	rc := step.getRunContext()
	if rc != nil && rc.JobContainer != nil && rc.JobContainer.IsEnvironmentCaseInsensitive() {
		mergeIntoMapCaseInsensitive(*target, maps...)
	} else {
		mergeIntoMapCaseSensitive(*target, maps...)
	}
}

func mergeIntoMapCaseSensitive(target map[string]string, maps ...map[string]string) {
	for _, m := range maps {
		for k, v := range m {
			target[k] = v
		}
	}
}

func mergeIntoMapCaseInsensitive(target map[string]string, maps ...map[string]string) {
	foldKeys := make(map[string]string, len(target))
	for k := range target {
		foldKeys[strings.ToLower(k)] = k
	}
	for _, m := range maps {
		for k, v := range m {
			lk := strings.ToLower(k)
			if orig, exists := foldKeys[lk]; exists {
				target[orig] = v
			} else {
				target[k] = v
				foldKeys[lk] = k
			}
		}
	}
}

func symlinkJoin(filename, sym, parent string) (string, error) {
	dest := path.Join(path.Dir(filename), sym)
	prefix := path.Clean(parent) + "/"
	if strings.HasPrefix(dest, prefix) || prefix == "./" {
		return dest, nil
	}
	return "", fmt.Errorf("symlink tries to access file '%s' outside of '%s'", strings.ReplaceAll(dest, "'", "''"), strings.ReplaceAll(parent, "'", "''"))
}
