/**
 * Centralized error reporting for the admin UI.
 *
 * Replaces silent `catch {}` blocks so production errors are visible to
 * developers (console + a single telemetry hook) instead of being swallowed.
 * UI-facing messaging (toasts) stays at the call site where context is known.
 */

/** Optional telemetry sink; wire to a real backend later if desired. */
type Reporter = (context: string, error: unknown) => void;

let telemetry: Reporter | null = null;

/** Install a telemetry reporter (e.g. to forward errors to a backend). */
export function setErrorReporter(reporter: Reporter | null): void {
  telemetry = reporter;
}

/** Report a handled error with context. Never throws. */
export function reportError(context: string, error: unknown): void {
  // eslint-disable-next-line no-console
  console.error(`[teodb] ${context}:`, error);
  try {
    telemetry?.(context, error);
  } catch {
    // A failing telemetry sink must never break the UI.
  }
}

export function useErrorLog() {
  return { reportError, setErrorReporter };
}
