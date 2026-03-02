import { createRoot } from "react-dom/client";
import "maplibre-gl/dist/maplibre-gl.css";
import "./styles.css";
import { CurationPage } from "./curation-page";

const root = document.getElementById("root");
if (!root) {
  throw new Error("Missing root element for curation page.");
}

createRoot(root).render(<CurationPage />);
