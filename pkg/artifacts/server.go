package artifacts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"

	"github.com/nektos/act/pkg/common"
)

const gzipExtension = ".gz__"

type FileContainerResourceURL struct {
	FileContainerResourceURL string `json:"fileContainerResourceUrl"`
}

type NamedFileContainerResourceURL struct {
	Name                     string `json:"name"`
	FileContainerResourceURL string `json:"fileContainerResourceUrl"`
}

type NamedFileContainerResourceURLResponse struct {
	Count int                             `json:"count"`
	Value []NamedFileContainerResourceURL `json:"value"`
}

type ContainerItem struct {
	Path            string `json:"path"`
	ItemType        string `json:"itemType"`
	ContentLocation string `json:"contentLocation"`
}

type ContainerItemResponse struct {
	Value []ContainerItem `json:"value"`
}

type ResponseMessage struct {
	Message string `json:"message"`
}

type WritableFile interface {
	io.WriteCloser
}

type WriteFS interface {
	OpenWritable(name string) (WritableFile, error)
	OpenAppendable(name string) (WritableFile, error)
}

type readWriteFSImpl struct{}

func (rw readWriteFSImpl) Open(name string) (fs.File, error) {
	return os.Open(name)
}

func (rw readWriteFSImpl) OpenWritable(name string) (WritableFile, error) {
	if err := os.MkdirAll(filepath.Dir(name), os.ModePerm); err != nil {
		return nil, err
	}
	return os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
}

func (rw readWriteFSImpl) OpenAppendable(name string) (WritableFile, error) {
	if err := os.MkdirAll(filepath.Dir(name), os.ModePerm); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func safeResolve(baseDir, relPath string) string {
	return filepath.Join(baseDir, filepath.Clean(filepath.Join(string(os.PathSeparator), relPath)))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	must(json.NewEncoder(w).Encode(v))
}

func postArtifactHandler(baseDir string) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		runID := ps.ByName("runId")

		resp := FileContainerResourceURL{
			FileContainerResourceURL: fmt.Sprintf("http://%s/upload/%s", req.Host, runID),
		}
		writeJSON(w, resp)
	}
}

func uploadHandler(baseDir string, fsys WriteFS) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		runID := ps.ByName("runId")
		itemPath := req.URL.Query().Get("itemPath")
		if req.Header.Get("Content-Encoding") == "gzip" {
			itemPath += gzipExtension
		}

		safeRunPath := safeResolve(baseDir, runID)
		safePath := safeResolve(safeRunPath, itemPath)

		var file WritableFile
		var err error

		contentRange := req.Header.Get("Content-Range")
		if contentRange != "" && !strings.HasPrefix(contentRange, "bytes 0-") {
			file, err = fsys.OpenAppendable(safePath)
		} else {
			file, err = fsys.OpenWritable(safePath)
		}
		must(err)
		defer file.Close()

		if req.Body == nil {
			must(errors.New("no body given"))
		}

		_, err = io.Copy(file, req.Body)
		must(err)

		writeJSON(w, ResponseMessage{Message: "success"})
	}
}

func patchArtifactHandler() httprouter.Handle {
	return func(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
		writeJSON(w, ResponseMessage{Message: "success"})
	}
}

func setupUploadRoutes(router *httprouter.Router, baseDir string, fsys WriteFS) {
	router.POST("/_apis/pipelines/workflows/:runId/artifacts", postArtifactHandler(baseDir))
	router.PUT("/upload/:runId", uploadHandler(baseDir, fsys))
	router.PATCH("/_apis/pipelines/workflows/:runId/artifacts", patchArtifactHandler())
}

func listArtifactsHandler(baseDir string, fsys fs.FS) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		runID := ps.ByName("runId")
		safePath := safeResolve(baseDir, runID)

		entries, err := fs.ReadDir(fsys, safePath)
		must(err)

		var list []NamedFileContainerResourceURL
		for _, entry := range entries {
			list = append(list, NamedFileContainerResourceURL{
				Name:                     entry.Name(),
				FileContainerResourceURL: fmt.Sprintf("http://%s/download/%s", req.Host, runID),
			})
		}

		resp := NamedFileContainerResourceURLResponse{
			Count: len(list),
			Value: list,
		}
		writeJSON(w, resp)
	}
}

func downloadItemsHandler(baseDir string, fsys fs.FS) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		container := ps.ByName("container")
		itemPath := req.URL.Query().Get("itemPath")
		safePath := safeResolve(baseDir, filepath.Join(container, itemPath))

		var files []ContainerItem

		err := fs.WalkDir(fsys, safePath, func(entryPath string, entry fs.DirEntry, _ error) error {
			if !entry.IsDir() {
				rel, err := filepath.Rel(safePath, entryPath)
				must(err)

				// remove gzip suffix if present
				rel = strings.TrimSuffix(rel, gzipExtension)

				fullPath := filepath.Join(itemPath, rel)

				rel = filepath.ToSlash(rel)
				fullPath = filepath.ToSlash(fullPath)

				files = append(files, ContainerItem{
					Path:            fullPath,
					ItemType:        "file",
					ContentLocation: fmt.Sprintf("http://%s/artifact/%s/%s/%s", req.Host, container, itemPath, rel),
				})
			}
			return nil
		})
		must(err)

		resp := ContainerItemResponse{Value: files}
		writeJSON(w, resp)
	}
}

func artifactFileHandler(baseDir string, fsys fs.FS) httprouter.Handle {
	return func(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
		pathParam := ps.ByName("path")
		if len(pathParam) < 1 {
			http.Error(w, "path missing", http.StatusBadRequest)
			return
		}
		relPath := pathParam[1:] // remove leading '/'
		safePath := safeResolve(baseDir, relPath)

		file, err := fsys.Open(safePath)
		if err != nil {
			// fallback to gzip variant
			file, err = fsys.Open(safePath + gzipExtension)
			if err != nil {
				http.Error(w, "file not found", http.StatusNotFound)
				return
			}
			w.Header().Add("Content-Encoding", "gzip")
		}
		defer file.Close()

		_, err = io.Copy(w, file)
		must(err)
	}
}

func setupDownloadRoutes(router *httprouter.Router, baseDir string, fsys fs.FS) {
	router.GET("/_apis/pipelines/workflows/:runId/artifacts", listArtifactsHandler(baseDir, fsys))
	router.GET("/download/:container", downloadItemsHandler(baseDir, fsys))
	router.GET("/artifact/*path", artifactFileHandler(baseDir, fsys))
}

func Serve(ctx context.Context, artifactPath, addr, port string) context.CancelFunc {
	serverCtx, cancel := context.WithCancel(ctx)
	logger := common.Logger(serverCtx)

	if artifactPath == "" {
		return cancel
	}

	router := httprouter.New()
	logger.Debugf("Artifacts base path '%s'", artifactPath)

	fsys := readWriteFSImpl{}

	setupUploadRoutes(router, artifactPath, fsys)
	setupDownloadRoutes(router, artifactPath, fsys)
	RoutesV4(router, artifactPath, fsys, fsys)

	server := &http.Server{
		Addr:              fmt.Sprintf("%s:%s", addr, port),
		ReadHeaderTimeout: 2 * time.Second,
		Handler:           router,
	}

	go func() {
		logger.Infof("Start server on http://%s:%s", addr, port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal(err)
		}
	}()

	go func() {
		<-serverCtx.Done()
		if err := server.Shutdown(ctx); err != nil {
			logger.Errorf("Failed graceful shutdown: %v", err)
			server.Close()
		}
	}()

	return cancel
}