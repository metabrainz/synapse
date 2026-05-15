package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/metabrainz/synapse/internal/store/tenants"
)

type adminHandler struct {
	repo *tenants.Repo
}

func generateAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (h *adminHandler) create(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ID == "" || body.Name == "" {
		writeError(w, http.StatusBadRequest, "id and name are required")
		return
	}

	apiKey, err := generateAPIKey()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate api key")
		return
	}

	if err := h.repo.Insert(r.Context(), tenants.Tenant{
		ID:     body.ID,
		APIKey: apiKey,
		Name:   body.Name,
	}); err != nil {
		if errors.Is(err, tenants.ErrDuplicate) {
			writeError(w, http.StatusConflict, "tenant already exists")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to create tenant")
		return
	}
	// Return the key exactly once — caller must store it, it won't be shown again.
	writeJSON(w, http.StatusCreated, map[string]string{"id": body.ID, "api_key": apiKey})
}

func (h *adminHandler) list(w http.ResponseWriter, r *http.Request) {
	list, err := h.repo.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list tenants")
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (h *adminHandler) rotateKey(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	newKey, err := generateAPIKey()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate api key")
		return
	}

	if err := h.repo.RotateAPIKey(r.Context(), id, newKey); err != nil {
		if err == tenants.ErrNotFound {
			writeError(w, http.StatusNotFound, "tenant not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to rotate key")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"api_key": newKey})
}
