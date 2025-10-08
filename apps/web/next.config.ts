import type { NextConfig } from "next";

const isDev = process.env.NODE_ENV !== "production";
const API = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8000";

const devCsp = [
  "default-src 'self'",
  // Next.js dev server and many libs (e.g., React Refresh, Plotly) require eval in development.
  "script-src 'self' 'unsafe-eval' 'wasm-unsafe-eval' 'unsafe-inline'",
  // fetch/XHR to local API
  `connect-src 'self' ${API} ws: wss:`,
  // images/fonts/styles
  "img-src 'self' data: blob:",
  "style-src 'self' 'unsafe-inline'",
  "font-src 'self' data: https:",
].join("; ");

const prodCsp = [
  "default-src 'self'",
  "script-src 'self'",
  `connect-src 'self' ${API}`,
  "img-src 'self' data: blob:",
  "style-src 'self' 'unsafe-inline'",
  "font-src 'self' data: https:",
].join("; ");

const nextConfig: NextConfig = {
  async headers() {
    return [
      {
        source: "/:path*",
        headers: [
          {
            key: "Content-Security-Policy",
            value: isDev ? devCsp : prodCsp,
          },
        ],
      },
    ];
  },
};

export default nextConfig;
