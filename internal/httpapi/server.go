package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/orchestrator"
)

type Server struct {
	service *orchestrator.Service
	static  http.Handler
	auth    *GoogleAuth
}

func New(service *orchestrator.Service, static http.Handler) *Server {
	return &Server{service: service, static: static}
}

func NewWithAuth(service *orchestrator.Service, static http.Handler, auth *GoogleAuth) *Server {
	return &Server{service: service, static: static, auth: auth}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.health)
	if s.auth != nil {
		s.auth.RegisterRoutes(mux)
	}
	mux.HandleFunc("GET /api/snapshot", s.snapshot)
	mux.HandleFunc("GET /api/events", s.events)
	mux.HandleFunc("GET /api/events/stream", s.eventStream)
	mux.HandleFunc("POST /api/assistant", s.assistant)
	mux.HandleFunc("GET /api/tasks/lookup", s.lookupTask)
	mux.HandleFunc("POST /api/tasks", s.createTask)
	mux.HandleFunc("POST /api/tasks/clear-terminal", s.clearTerminalTasks)
	mux.HandleFunc("POST /api/tasks/{id}/clear", s.clearTask)
	mux.HandleFunc("POST /api/tasks/{id}/steer", s.steerTask)
	mux.HandleFunc("POST /api/tasks/{id}/cancel", s.cancelTask)
	mux.HandleFunc("POST /api/tasks/{id}/apply-policy", s.recommendApplyPolicy)
	mux.HandleFunc("POST /api/tasks/{id}/pull-request", s.publishTaskPullRequest)
	mux.HandleFunc("POST /api/pull-requests/{id}/refresh", s.refreshPullRequest)
	mux.HandleFunc("POST /api/pull-requests/{id}/babysit", s.startPullRequestBabysitter)
	mux.HandleFunc("GET /api/workers/{id}/changes", s.reviewWorkerChanges)
	mux.HandleFunc("POST /api/workers/{id}/apply", s.applyWorkerChanges)
	mux.HandleFunc("POST /api/workers/{id}/cancel", s.cancelWorker)
	if s.static != nil {
		mux.Handle("/", s.static)
	}
	var handler http.Handler = mux
	if s.auth != nil {
		handler = s.auth.Middleware(handler)
	}
	return withCORS(handler)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) snapshot(w http.ResponseWriter, r *http.Request) {
	snapshot, err := s.service.Snapshot(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	afterID := parseInt64(r.URL.Query().Get("after"))
	limit := int(parseInt64(r.URL.Query().Get("limit")))
	events, err := s.service.Events(r.Context(), afterID, limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	var req core.CreateTaskRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, err)
		return
	}
	task, err := s.service.CreateTask(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, task)
}

func (s *Server) assistant(w http.ResponseWriter, r *http.Request) {
	var req core.AssistantRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, err)
		return
	}
	response, err := s.service.Ask(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) lookupTask(w http.ResponseWriter, r *http.Request) {
	task, ok, err := s.service.FindTaskByExternalID(r.Context(), r.URL.Query().Get("source"), r.URL.Query().Get("externalId"))
	if err != nil {
		writeError(w, err)
		return
	}
	if !ok {
		writeError(w, eventstore.ErrNotFound)
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *Server) steerTask(w http.ResponseWriter, r *http.Request) {
	var req core.SteeringRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, err)
		return
	}
	if err := s.service.SteerTask(r.Context(), r.PathValue("id"), req); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) cancelTask(w http.ResponseWriter, r *http.Request) {
	if err := s.service.CancelTask(r.Context(), r.PathValue("id")); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) clearTask(w http.ResponseWriter, r *http.Request) {
	if err := s.service.ClearTask(r.Context(), r.PathValue("id")); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) clearTerminalTasks(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.ClearTerminalTasks(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) cancelWorker(w http.ResponseWriter, r *http.Request) {
	if err := s.service.CancelWorker(r.Context(), r.PathValue("id")); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) reviewWorkerChanges(w http.ResponseWriter, r *http.Request) {
	review, err := s.service.ReviewWorkerChanges(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, review)
}

func (s *Server) applyWorkerChanges(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.ApplyWorkerChanges(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) recommendApplyPolicy(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.RecommendApplyPolicy(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) publishTaskPullRequest(w http.ResponseWriter, r *http.Request) {
	var req core.PublishPullRequestRequest
	if r.Body != nil && r.ContentLength != 0 {
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, err)
			return
		}
	}
	result, err := s.service.PublishTaskPullRequest(r.Context(), r.PathValue("id"), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) refreshPullRequest(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.RefreshPullRequest(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) startPullRequestBabysitter(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.StartPullRequestBabysitter(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) eventStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, errors.New("streaming is not supported"))
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	subID, events := s.service.Subscribe()
	defer s.service.Unsubscribe(subID)

	afterID := parseInt64(r.URL.Query().Get("after"))
	initial, err := s.service.Events(r.Context(), afterID, 1000)
	if err != nil {
		writeSSE(w, "error", map[string]string{"error": err.Error()})
		flusher.Flush()
		return
	}
	for _, event := range initial {
		writeSSE(w, "event", event)
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-events:
			writeSSE(w, "event", event)
			flusher.Flush()
		}
	}
}

func decodeJSON(r *http.Request, out any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(out)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, eventstore.ErrNotFound) {
		status = http.StatusNotFound
	} else if strings.Contains(err.Error(), "not allowed") {
		status = http.StatusForbidden
	} else if strings.Contains(err.Error(), "oauth") || strings.Contains(err.Error(), "id token") || strings.Contains(err.Error(), "email is not verified") {
		status = http.StatusUnauthorized
	} else if strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "unknown field") || strings.Contains(err.Error(), "terminal") || strings.Contains(err.Error(), "multiple unapplied") {
		status = http.StatusBadRequest
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeSSE(w http.ResponseWriter, eventName string, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		data = []byte(fmt.Sprintf(`{"error":%q}`, err.Error()))
	}
	_, _ = fmt.Fprintf(w, "event: %s\n", eventName)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
}

func parseInt64(value string) int64 {
	if value == "" {
		return 0
	}
	parsed, _ := strconv.ParseInt(value, 10, 64)
	return parsed
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "content-type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
