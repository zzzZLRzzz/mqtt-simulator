package config

import (
	"bytes"
	"fmt"
	mrand "math/rand"
	"sync"
	"text/template"
	"time"
)

var (
	templateCache   = make(map[string]*template.Template)
	templateCacheMu sync.Mutex

	rngPool = sync.Pool{
		New: func() any {
			return mrand.New(mrand.NewSource(time.Now().UnixNano()))
		},
	}
)

type TemplateData struct {
	ClientID       string
	MessageTopic   string
	MessagePayload string
}

func getFuncMap(rng *mrand.Rand) template.FuncMap {
	return template.FuncMap{
		"RandomInt": func(min, max int) int {
			return rng.Intn(max-min+1) + min
		},
		"RandomFloat": func(min, max float64) float64 {
			return min + rng.Float64()*(max-min)
		},
		"Now": func() time.Time {
			return time.Now()
		},
		"NowUnix": func() int64 {
			return time.Now().Unix()
		},
	}
}

func ExecuteTemplate(tmpl string, data TemplateData) (string, error) {
	if tmpl == "" {
		return "", nil
	}

	rng := rngPool.Get().(*mrand.Rand)
	defer rngPool.Put(rng)

	t, err := getOrCreateTemplate(tmpl, rng)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, &data); err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return buf.String(), nil
}

func getOrCreateTemplate(tmplStr string, rng *mrand.Rand) (*template.Template, error) {
	templateCacheMu.Lock()
	t, ok := templateCache[tmplStr]
	templateCacheMu.Unlock()

	if ok {
		cloned, err := t.Clone()
		if err != nil {
			return nil, fmt.Errorf("failed to clone template: %w", err)
		}
		cloned = cloned.Funcs(getFuncMap(rng))
		return cloned, nil
	}

	t, err := template.New("").Funcs(getFuncMap(rng)).Parse(tmplStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse template: %w", err)
	}

	templateCacheMu.Lock()
	if cached, ok := templateCache[tmplStr]; ok {
		templateCacheMu.Unlock()
		cloned, _ := cached.Clone()
		cloned = cloned.Funcs(getFuncMap(rng))
		return cloned, nil
	}
	templateCache[tmplStr] = t
	templateCacheMu.Unlock()

	return t, nil
}

func ExecutePayloadTemplate(tmpl string, clientID string) (string, error) {
	return ExecuteTemplate(tmpl, TemplateData{ClientID: clientID})
}
