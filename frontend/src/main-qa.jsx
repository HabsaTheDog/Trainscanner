import { createRoot } from "react-dom/client";
import "maplibre-gl/dist/maplibre-gl.css";
import "./styles.css";
import { QAQueuePage } from "./QAQueuePage";

const root = document.getElementById("root");
if (!root) {
    throw new Error("Missing root element for QA queue page.");
}

createRoot(root).render(<QAQueuePage />);
