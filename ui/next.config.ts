import type { NextConfig } from "next";
import { config } from "dotenv";
import path from "path";

// Load environment variables from parent directory's .env file
config({ path: path.resolve(__dirname, "../.env") });

const nextConfig: NextConfig = {
  /* config options here */
};

export default nextConfig;
