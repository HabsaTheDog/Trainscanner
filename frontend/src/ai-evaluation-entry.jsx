import { createRoot } from "react-dom/client";
import "./styles.css";
import { AiEvaluationPage } from "./ai-evaluation-page";

const root = document.getElementById("root");
if (!root) {
  throw new Error("Missing root element for AI evaluation page.");
}

createRoot(root).render(<AiEvaluationPage />);
