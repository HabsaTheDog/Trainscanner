import { useEffect } from "react";
import { initCurationApp } from "./curation-page-runtime";

export function useCurationPageRuntime() {
  useEffect(() => {
    const cleanup = initCurationApp();
    return () => {
      if (typeof cleanup === "function") {
        cleanup();
      }
    };
  }, []);
}
