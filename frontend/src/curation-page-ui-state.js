import { parseRef } from "./curation-page-runtime.js";

export function createUiState() {
  return {
    selectedRefs: new Set(),
    focusedRef: "",
    activeTool: "merge",
    mapMode: "default",
    lastSelectedIndex: -1,
    bottomTab: "candidates",
  };
}

function resolveFocusedTool(ref, fallbackTool) {
  const parsed = parseRef(ref);
  if (parsed.type === "group") {
    return "group";
  }
  if (parsed.type === "merge") {
    return "merge";
  }
  return fallbackTool;
}

export function uiReducer(state, action) {
  switch (action.type) {
    case "clear_selection":
      return { ...state, selectedRefs: new Set(), lastSelectedIndex: -1 };
    case "set_selection":
      return {
        ...state,
        selectedRefs: new Set(action.refs || []),
        lastSelectedIndex: Number.isFinite(action.lastSelectedIndex)
          ? action.lastSelectedIndex
          : state.lastSelectedIndex,
      };
    case "toggle_selection": {
      const selectedRefs = new Set(state.selectedRefs);
      if (selectedRefs.has(action.ref)) {
        selectedRefs.delete(action.ref);
      } else {
        selectedRefs.add(action.ref);
      }
      return {
        ...state,
        selectedRefs,
        lastSelectedIndex: Number.isFinite(action.index)
          ? action.index
          : state.lastSelectedIndex,
      };
    }
    case "focus":
      return {
        ...state,
        focusedRef: action.ref || "",
        activeTool:
          action.tool || resolveFocusedTool(action.ref, state.activeTool),
      };
    case "tool":
      return { ...state, activeTool: action.tool };
    case "map_mode":
      return { ...state, mapMode: action.mode };
    case "bottom_tab":
      return { ...state, bottomTab: action.tab };
    default:
      return state;
  }
}
