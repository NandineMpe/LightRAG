/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_PROXY: string
  readonly VITE_API_ENDPOINTS: string
  readonly VITE_BACKEND_URL: string
  readonly VITE_KLAVIYO_PUBLIC_KEY?: string
  readonly VITE_KLAVIYO_FORM_ID?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
