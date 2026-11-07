package observability

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ─── HTTP Middleware ──────────────────────────────────────────────────────────

// HTTPMiddleware es un middleware HTTP que:
//  1. Extrae el trace context de los headers W3C Trace-Context entrantes
//  2. Crea un span para la request
//  3. Inyecta el logger enriquecido con trace_id en el contexto
//  4. Registra la duración y el status code al finalizar
func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extraer trace context de headers entrantes (W3C traceparent)
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))

		// Crear span para esta request
		ctx, span := StartSpan(ctx, r.Method+" "+r.URL.Path,
			trace.WithAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.url", r.URL.String()),
				attribute.String("http.host", r.Host),
			),
		)
		defer span.End()

		// Enriquecer el contexto con el logger
		ctx = WithContext(ctx,
			slog.String("http.method", r.Method),
			slog.String("http.path", r.URL.Path),
		)

		// Wrapper para capturar el status code
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		start := time.Now()
		next.ServeHTTP(rw, r.WithContext(ctx))
		duration := time.Since(start)

		// Anotar resultado en el span
		span.SetAttributes(
			attribute.Int("http.status_code", rw.statusCode),
			attribute.Int64("http.duration_ms", duration.Milliseconds()),
		)

		// Log de la request
		log := FromContext(ctx)
		log.Info("http request",
			slog.Int("status", rw.statusCode),
			slog.Int64("duration_ms", duration.Milliseconds()),
		)
	})
}

// responseWriter captura el status code del ResponseWriter original.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

// Unwrap deja que http.ResponseController llegue al ResponseWriter original: lo necesita un
// proxy para pasar una conexión a WebSocket o limpiar sus deadlines.
func (rw *responseWriter) Unwrap() http.ResponseWriter { return rw.ResponseWriter }

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// ─── gRPC Interceptors ────────────────────────────────────────────────────────

// GRPCUnaryServerInterceptor extrae el trace context de los metadatos gRPC
// entrantes y crea un span para cada llamada unaria.
func GRPCUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// Extraer trace context desde los metadatos gRPC
		ctx = extractGRPCTraceContext(ctx)

		ctx, span := StartSpan(ctx, info.FullMethod,
			trace.WithAttributes(attribute.String("rpc.method", info.FullMethod)),
		)
		defer span.End()

		ctx = WithContext(ctx, slog.String("grpc.method", info.FullMethod))

		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)

		span.SetAttributes(attribute.Int64("rpc.duration_ms", duration.Milliseconds()))

		if err != nil {
			RecordError(ctx, err)
			st, _ := status.FromError(err)
			span.SetAttributes(attribute.String("rpc.grpc_status", st.Code().String()))
		}

		return resp, err
	}
}

// GRPCUnaryClientInterceptor inyecta el trace context en los metadatos gRPC
// salientes para propagar la traza hacia el servidor destino.
func GRPCUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		ctx, span := StartSpan(ctx, method,
			trace.WithAttributes(attribute.String("rpc.method", method)),
		)
		defer span.End()

		// Inyectar trace context en los metadatos gRPC salientes
		ctx = injectGRPCTraceContext(ctx)

		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			RecordError(ctx, err)
		}
		return err
	}
}

// ─── gRPC trace context helpers ──────────────────────────────────────────────

// grpcMetadataCarrier adapta grpc.metadata.MD para que sea compatible
// con la interfaz TextMapCarrier de OpenTelemetry.
type grpcMetadataCarrier struct{ md metadata.MD }

func (c grpcMetadataCarrier) Get(key string) string {
	vals := c.md.Get(key)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

func (c grpcMetadataCarrier) Set(key, val string) {
	c.md.Set(key, val)
}

func (c grpcMetadataCarrier) Keys() []string {
	keys := make([]string, 0, len(c.md))
	for k := range c.md {
		keys = append(keys, k)
	}
	return keys
}

func extractGRPCTraceContext(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, grpcMetadataCarrier{md: md})
}

func injectGRPCTraceContext(ctx context.Context) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	}
	otel.GetTextMapPropagator().Inject(ctx, grpcMetadataCarrier{md: md})
	return metadata.NewOutgoingContext(ctx, md)
}
