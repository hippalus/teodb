/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** TeoDB server origin (e.g. `https://teodb.example.com`). Empty = same origin. */
  readonly VITE_TEODB_SERVER_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

interface Window {
  /** Runtime override for the TeoDB server origin (set before the app loads). */
  __TEODB_SERVER_URL__?: string;
}
