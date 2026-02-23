import { createRoot } from "react-dom/client";
import "maplibre-gl/dist/maplibre-gl.css";
import "./styles.css";
import { HomePage } from "./HomePage";

const root = document.getElementById("root");
if (!root) {
  throw new Error("Missing root element for home page.");
}

createRoot(root).render(<HomePage />);
