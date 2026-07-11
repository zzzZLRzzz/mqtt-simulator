package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

type LogLevel string

const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

func (l LogLevel) ToSlogLevel() slog.Level {
	switch strings.ToLower(string(l)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func (l LogLevel) String() string {
	return string(l)
}

type simpleHandler struct {
	w     io.Writer
	level slog.Level
}

func (h *simpleHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *simpleHandler) Handle(ctx context.Context, r slog.Record) error {
	timeStr := r.Time.Format("2006-01-02 15:04:05.000")
	msg := r.Message
	fmt.Fprintf(h.w, "[%s] [%s] %s\n", timeStr, r.Level.String(), msg)
	return nil
}

func (h *simpleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *simpleHandler) WithGroup(name string) slog.Handler {
	return h
}

type Logger struct {
	slogger *slog.Logger
	level   LogLevel
	prefix  string
}

func NewLogger(level LogLevel, prefix string) *Logger {
	slogLevel := level.ToSlogLevel()
	handler := &simpleHandler{
		w:     os.Stdout,
		level: slogLevel,
	}
	slogger := slog.New(handler)
	return &Logger{
		slogger: slogger,
		level:   level,
		prefix:  prefix,
	}
}

func (l *Logger) Debug(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	l.slogger.Debug(msg)
}

func (l *Logger) Info(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	l.slogger.Info(msg)
}

func (l *Logger) Warn(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	l.slogger.Warn(msg)
}

func (l *Logger) Error(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	l.slogger.Error(msg)
}

func (l *Logger) Printf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	l.slogger.Info(msg)
}

func (l *Logger) Print(args ...any) {
	l.slogger.Info(fmt.Sprint(args...))
}

func (l *Logger) Println(args ...any) {
	l.slogger.Info(fmt.Sprint(args...))
}

func (l *Logger) Fatalf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	l.slogger.Error(msg)
	os.Exit(1)
}

func (l *Logger) Level() LogLevel {
	return l.level
}

func (l *Logger) IsDebug() bool {
	return l.level.ToSlogLevel() <= slog.LevelDebug
}

func (l *Logger) IsInfo() bool {
	return l.level.ToSlogLevel() <= slog.LevelInfo
}

func (l *Logger) IsWarn() bool {
	return l.level.ToSlogLevel() <= slog.LevelWarn
}

func (l *Logger) IsError() bool {
	return true
}
