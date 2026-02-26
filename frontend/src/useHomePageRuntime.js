import { useEffect } from "react";
import { initHomeApp } from "./home-page-runtime";

export function useHomePageRuntime() {
  useEffect(() => {
    const cleanup = initHomeApp();
    return () => {
      if (typeof cleanup === "function") {
        cleanup();
      }
    };
  }, []);
}
