import asyncio
from nicegui import ui, app
import json
import psycopg2
import pandas as pd
from sshtunnel import SSHTunnelForwarder
import traceback
import logging
from typing import Dict, Any, Optional, Tuple, List
import uuid
from datetime import datetime
import sys
from io import StringIO
import os
from pathlib import Path
import numpy as np
import time
import functools
import matplotlib
matplotlib.use('Agg') # Use 'Agg' for PNG output (non-interactive)
import matplotlib.pyplot as plt
import io # NEW: For in-memory binary streams
import base64 # NEW: For base64 encoding images

# Attempt to import tkinter for native directory picker
try:
    import tkinter
    from tkinter import filedialog
    TKINTER_AVAILABLE = True
except ImportError:
    TKINTER_AVAILABLE = False

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not TKINTER_AVAILABLE:
    logger.warning("tkinter module not found. Native directory picker will be disabled.")

# Custom CSS (no changes needed here for functionality, but keeping it for context)
# MODIFIED: Removed the 'add new cell' logic from Shift+Enter in the JavaScript
custom_css = """
<style>
* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

html, body {
    margin: 0;
    padding: 0;
    width: 100%;
    min-height: 100%;
}

:root {
    --bg-primary: #ffffff;
    --bg-secondary: #f8f8f8;
    --bg-tertiary: #ffffff;
    --bg-output: #f9f9f9;
    --text-primary: #000000;
    --text-secondary: #333333;
    --border-color: #e0e0e0;
    --input-bg: #ffffff;
    --text-primary-rgb: 0,0,0;
}

.dark-mode {
    --bg-primary: #121212;
    --bg-secondary: #121212;
    --bg-tertiary: #2d2d2d;
    --bg-output: #1a1a1a;
    --text-primary: #ffffff;
    --text-secondary: #e0e0e0;
    --border-color:  #444444;
    --input-bg: #2d2d2d;
    --text-primary-rgb: 255,255,255;
}

body {
    background-color: var(--bg-secondary);
    color: var(--text-primary);
    transition: background-color 0.3s, color 0.3s;
}

.main-container {
    background-color: var(--bg-secondary);
    color: var(--text-primary);
    min-height: 100vh;
    margin: 0;
    padding: 0;
}

.code-cell {
    border: 1px solid var(--border-color);
    border-top-left-radius: 16px;   /* Round top corners */
    border-top-right-radius: 16px; /* Apply overall rounding here */
    margin-bottom: 8px;
    background-color: var(--bg-tertiary); /* Base background for the cell */
    transition: all 0.3s ease;
    max-width: 800px;
    margin-left: 0px;
    margin-right: auto;
}

.code-cell.collapsed {
    margin-bottom: 0px;
}

.code-cell-header {
    display: flex;
    border-top-left-radius: 16px;   /* Round top corners */
    border-top-right-radius: 16px;  /* Round top corners */
    align-items: center;
    background-color: var(--bg-tertiary); /* Header background */
    border-bottom: 1px solid #fff8e6; /* Separator line */
}

.code-cell.collapsed .code-cell-header {
    border-bottom: none; /* No separator when collapsed */
    border-bottom-left-radius: 16px;  /* Round bottom corners when collapsed */
    border-bottom-right-radius: 16px; /* Round bottom corners when collapsed */
}

.code-cell-content {
    transition: all 0.3s ease;
    overflow: hidden; /* Ensure internal clipping */
    /* No specific background here, depends on cm-editor or output-area */
}

.code-cell.collapsed .code-cell-content {
    max-height: 0;
    opacity: 0;
}

.collapse-button {
    background: none;
    border: none;
    color: #5898D4;
    cursor: pointer;
    padding: 6px;
    border-radius: 3px;
    font-size: 16px;
    transition: all 0.2s ease;
    margin-right: 8px;
    display: flex;
    align-items: center;
    justify-content: center;
    min-width: 24px;
    height: 24px;
}

.collapse-button:hover {
    background-color: var(--border-color);
    color: #4a7bb8;
}

.collapse-icon {
    transition: transform 0.3s ease;
    font-size: 16px;
}

.collapsed .collapse-icon {
    transform: rotate(-180deg);
}

.output-area {
    border-bottom-left-radius: 16px;  /* Round bottom-left for output */
    border-bottom-right-radius: 16px; /* Round bottom-right for output */
    background-color: var(--bg-primary); /* Output background */
    border-top: 1px solid var(--border-color); /* Separates output from editor */
    font-family: monospace;
    font-size: 14px;
    max-height: 700px;
    overflow-y: auto;
    color: var(--text-primary);
    padding-top: 12px;
    padding-bottom: 12px;
    padding-left: 12px;
    padding-right: 12px;

}

.output-area > div {
    overflow-x: auto;
}

.execution-status {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-left: 12px;
    color: var(--text-secondary);
    font-size: 14px;
}

.timer-text {
    font-family: monospace;
    color: var(--text-secondary);
}

.execution-result {
    display: flex;
    align-items: center;
    gap: 6px;
    margin-left: 12px;
    font-size: 14px;
    font-family: monospace;
}

.result-success {
    color: #4caf50;
}

.result-error {
    color: #f44336;
}

.result-icon {
    font-size: 20px;
    font-weight: bold;
}

.run-button {
    margin-left: 12px;
    font-size: 12px !important;
    padding: 4px 8px !important;
    min-height: 28px !important;
    height: 28px !important;
}

.delete-button {
    margin-right: 12px;
    font-size: 16px !important;
    padding: 0px 0px !important;
    min-height: 27px !important;
    height: 27px !important;
    min-width: 27px !important;
    width: 27px !important;
    transition: all 0.2s ease;
}

.delete-button:hover {
    transform: scale(1.1);
    box-shadow: 0 4px 12px rgba(0,0,0,0.25);
}

.save-button {
    font-size: 14px !important;
    padding: 0px 0px !important;
    min-height: 28px !important;
    height: 28px !important;
    min-width: 30px !important;
    width: 30px !important;
    transition: all 0.3s ease;
}

.save-button:hover {
    transform: scale(1.1);
    box-shadow: 0 4px 12px rgba(0,0,0,0.25);
}


.toolbar {
    display: flex;
    gap: 12px;
    padding: 10px 22px;
    height: 50px;
    background-color: var(--bg-tertiary);
    border-bottom: 1px solid var(--border-color);
    color: var(--text-primary);
}

.connection-status { 
    display: flex; 
    align-items: center; 
    gap: 8px; 
    margin-left: auto; 
}

.status-indicator { 
    width: 10px; 
    height: 10px; 
    border-radius: 50%; 
}

.status-connected { 
    background-color: #4caf50; 
}

.status-disconnected { 
    background-color: #f44336; 
}

/* CHANGES HERE for CodeMirror */
.code-editor { /* This is the outer NiceGUI wrapper for CodeMirror */
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    font-size: 16px;
    line-height: 1.5;
    background-color: var(--input-bg) !important; /* Set editor background */
    color: var(--text-primary) !important;
    min-height: 100px !important;
    height: auto !important;
    resize: none;
    /* No border on .code-editor itself, it's just a wrapper */
}



.cm-editor { /* This is the actual CodeMirror root element */
    min-height: 100px !important;
    height: auto !important;
    border: none !important; 
    background-color: var(--input-bg) !important; 
    border-bottom-left-radius: 16px; 
    border-bottom-right-radius: 16px; 
}

.cm-scroller {
    min-height: 100px !important;
    overflow-y: auto !important;
}

.code-cell.collapsed .code-editor,
.code-cell.collapsed .cm-editor {
    min-height: 36px !important;
}

.dataframe {
    border-collapse: collapse;
    margin: 10px 0;
    font-size: 0.9em;
    font-family: Arial, sans-serif;
    min-width: 300px;
    box-shadow: 0 0 10px rgba(0, 0, 0, 0.05);
    width: auto;
    max-width: 100%;
    border: 1px solid var(--border-color);
}

.dataframe thead tr {
    background-color: var(--bg-tertiary);
    color: var(--text-primary);
    text-align: left;
    font-weight: bold;
}

.dataframe th,
.dataframe td {
    padding: 8px 12px;
    border: 1px solid var(--border-color);
    color: var(--text-secondary);
    white-space: nowrap;
}

.dataframe tbody tr {
    border-bottom: 1px solid var(--border-color);
    background-color: var(--bg-primary);
}

.dataframe tbody tr:nth-of-type(even) {
    background-color: var(--bg-output);
}

.dark-mode .dataframe thead tr {
    background-color: var(--bg-tertiary);
    color: var(--text-primary);
}

.dark-mode .dataframe th,
.dark-mode .dataframe td {
    color: var(--text-secondary);
    border-color: var(--border-color);
}

.dark-mode .dataframe tbody tr {
    background-color: var(--bg-primary);
}

.dark-mode .dataframe tbody tr:nth-of-type(even) {
    background-color: var(--bg-output);
}

.dark-mode .q-field__control { 
    background-color: var(--input-bg) !important; 
    color: var(--text-primary) !important; 
}

.dark-mode .q-field__native { 
    color: var(--text-primary) !important; 
}

.dark-mode .q-textarea .q-field__native { 
    color: var(--text-primary) !important; 
}

.dark-mode .q-select__dropdown-icon { 
    color: var(--text-primary) !important; 
}

.dark-mode .q-select { 
    color: var(--text-primary) !important; 
}

.dark-mode .q-select .q-field__control { 
    background-color: var(--input-bg) !important; 
    color: var(--text-primary) !important; 
}

.dark-mode .q-select .q-field__native { 
    color: var(--text-primary) !important; 
}

.dark-mode .q-select .q-field__label { 
    color: var(--text-primary) !important; 
}

.dark-mode .q-select .q-field__control:before { 
    border-color: var(--border-color) !important; 
}

.dark-mode .q-select .q-field__control:hover:before { 
    border-color: var(--text-secondary) !important; 
}

.dark-mode .q-menu { 
    background-color: var(--bg-tertiary) !important; 
    color: var(--text-primary) !important; 
}

.dark-mode .q-item { 
    color: var(--text-primary) !important; 
}

.dark-mode .q-item:hover { 
    background-color: var(--bg-primary) !important; 
}

.dark-mode .q-item__label { 
    color: var(--text-primary) !important; 
}

.dark-mode .q-card { 
    background-color: var(--bg-tertiary) !important; 
    color: var(--text-primary) !important; 
}

.dark-mode .q-input { 
    color: var(--text-primary) !important; 
}

.dark-mode .q-input .q-field__control { 
    background-color: var(--input-bg) !important; 
    color: var(--text-primary) !important; 
}

.dark-mode label { 
    color: var(--text-primary) !important; 
}

.dark-mode .q-placeholder::placeholder { 
    color: var(--text-secondary) !important; 
    opacity: 0.6; 
}

.dark-mode ::placeholder { 
    color: var(--text-secondary) !important; 
    opacity: 0.6; 
}

.cell-container {
    background-color: var(--bg-secondary);
    padding: 16px 30px 16px 75px;
    overflow-y: auto;
    min-height: 0; 
}

.nicegui-content { 
    margin: 0 !important; 
    padding: 0 !important; 
}

.q-page { 
    padding: 0 !important; 
}

/* Specific dark mode adjustments for elements inside CodeMirror */
.dark-mode .cm-editor { 
    background-color: var(--input-bg); 
    color: var(--text-primary); 
    /* border-color was here, but `border: none !important;` above will remove it */
}

.dark-mode .cm-gutters { 
    background-color: var(--bg-tertiary); 
    color: var(--text-secondary); 
    border-right: 1px solid var(--border-color); 
    border-bottom-left-radius: 16px; /* <--- Add this for gutter rounding */
}

.dark-mode .cm-content { 
    color: var(--text-primary); 
}

.dark-mode .cm-cursor { 
    border-left-color: var(--text-primary); 
}

.dark-mode .cm-activeLine { 
    background-color: rgba(128, 128, 128, 0.2); 
}

.dark-mode .cm-activeLineGutter { 
    background-color: rgba(128, 128, 128, 0.2); 
}

.dark-mode .cm-selectionBackground { 
    background-color: rgba(var(--text-primary-rgb), 0.3) !important; 
}

.dark-mode .cm-focused .cm-selectionBackground { 
    background-color: rgba(var(--text-primary-rgb), 0.4) !important; 
}

.cell-preview {
    padding: 4px 8px;
    font-size: 12px;
    color: var(--text-secondary);
    font-family: monospace;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 400px;
    background-color: rgba(var(--text-primary-rgb), 0.05);
    border-radius: 3px;
    margin-left: 8px;
}

.header-control-padding {
    padding-top: 1px !important;
    padding-bottom: 1px !important;
}

.cell-with-gutter {
    display: flex;
    position: relative;
}

.cell-gutter {
    position: absolute;
    left: -50px;
    bottom: 8px;
    width: 40px;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 4px;
    z-index: 10;
    transition: all 0.3s ease;
}

.code-cell.collapsed .cell-gutter {
    bottom: auto;
    top: 50%;
    transform: translateY(-45%);
}

.gutter-run-button {
    width: 35px !important;
    height: 35px !important;
    min-height: 35px !important;
    border-radius: 50% !important;
    padding: 0 !important;
    font-size: 18px !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.15);
    transition: all 0.2s ease;
}

.gutter-run-button:hover {
    transform: scale(1.1);
    box-shadow: 0 4px 12px rgba(0,0,0,0.25);
}

.drawer-button-hover-effect {
    transition: all 0.2s ease; /* Smooth transition for scale and shadow */
}

.drawer-button-hover-effect:hover {
    transform: scale(1.1); /* Expands to 110% on hover */
}

.browse-wd-button-hover-effect {
    transition: all 0.2s ease; /* Smooth transition */
}

.browse-wd-button-hover-effect:hover {
    transform: scale(1.1); /* Expands on hover */
    box-shadow: 0 4px 12px rgba(0,0,0,0.25); /* Adds a larger shadow */
}

.gutter-execution-status {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 2px;
    font-size: 10px;
}

.gutter-execution-result {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 2px;
    font-size: 10px;
}

.gutter-result-icon {
    font-size: 16px !important;
    font-weight: bold;
}

.gutter-timer-text {
    font-family: monospace;
    color: var(--text-secondary);
    font-size: 12px;
}

.file-explorer-header { 
    background-color: var(--bg-tertiary);
    border-bottom: 1px solid var(--border-color);
    font-weight: normal; 
    font-size: 0.8rem; 
    color: var(--text-primary);
}

.dark-mode .file-explorer-header {
    color: var(--text-primary) !important;
    background-color: var(--bg-tertiary) !important;
    border-bottom-color: var(--border-color) !important;
}

.working-directory-input-row { 
    border-bottom: 1px solid var(--border-color);
    background-color: var(--bg-tertiary); 
}

.dark-mode .working-directory-input-row {
    background-color: var(--bg-tertiary) !important;
    border-bottom-color: var(--border-color) !important;
}

.file-tree-container {
    overflow-y: auto;
    background-color: var(--bg-primary);
}

.dark-mode .file-tree-container {
    background-color: var(--bg-primary) !important;
}

.dark-mode .q-drawer {
    background-color: var(--bg-primary) !important;
    border-right: 1px solid var(--border-color) !important;
    color: var(--text-primary) !important;
}

.dark-mode .q-tree {
    color: var(--text-primary) !important;
}

.dark-mode .q-tree .q-tree__node-header-content > div {
    color: var(--text-primary) !important; /* General rule for text */
}

.dark-mode .q-tree .q-tree__arrow,
.dark-mode .q-tree .q-tree__icon {
    color: var(--text-primary) !important; /* General rule for icons */
}

.dark-mode .q-tree__node:before,
.dark-mode .q-tree__node:after {
    border-color: var(--text-secondary) !important;
}

.dark-mode .q-tree .q-tree__node-header:hover {
    background-color: var(--bg-output) !important;
}

.dark-mode .q-tree .q-tree__node-header:hover .q-tree__node-header-content > div,
.dark-mode .q-tree .q-tree__node-header:hover .q-tree__arrow,


.q-drawer .nicegui-column {
    height: 100%;
}

.notebook-controls {
    display: flex;
    gap: 8px;
}

.save-load-button {
    font-size: 12px !important;
    padding: 2px 2px 1px 1px !important;
    min-height: 24px !important;
    height: 24px !important;
    min-width: 55px !important;
    width:55px !important;
    border-radius: 16px !important;
    transition: all 0.3s ease;
}

.save-load-button:hover {
    transform: scale(1.1);
    box-shadow: 0 4px 12px rgba(0,0,0,0.25);
}

.connect-button {
    font-size: 12px !important;
    padding: 2px 2px 1px 1px !important;
    min-height: 24px !important;
    height: 24px !important;
    min-width: 80px !important;
    width:80px !important;
    border-radius: 16px !important;
    transition: all 0.3s ease;
}

.connect-button:hover {
    transform: scale(1.1);
    box-shadow: 0 4px 12px rgba(0,0,0,0.25);
}

.add-cell-button {
    font-size: 13px !important;
    padding: 2px 4px !important;
    min-height: 32px !important;
    height: 32px !important;
    min-width: 90px !important;
    width:90px !important;
    border-radius: 18px !important;
    transition: all 0.3s ease;
}

.add-cell-button:hover {
    transform: scale(1.1);
    box-shadow: 0 4px 12px rgba(0,0,0,0.25);
}

/* MODIFICATION START */
/* Custom CSS for .dnb files */
.dnb-file-node .q-icon,
.dnb-file-node .q-tree__node-header-content > div {
    color: orange !important;
}

/* Ensure color applies in dark mode too */
.dark-mode .dnb-file-node .q-icon,
.dark-mode .dnb-file-node .q-tree__node-header-content > div {
    color: orange !important;
}

/* Hover state for .dnb files */
.q-tree__node-header:hover .dnb-file-node .q-icon,
.q-tree__node-header:hover .dnb-file-node .q-tree__node-header-content > div {
    color: #ff8c00 !important; /* Darker orange on hover */
}
/* MODIFICATION END */

</style>


<script>
// --- Keyboard Shortcut Handling (Ctrl+Enter, Shift+Enter, Ctrl+Down Arrow) ---
document.addEventListener('DOMContentLoaded', function() {
    console.log('Setting up keyboard shortcuts...');
    
    document.addEventListener('keydown', function(e) {
        // Ctrl+Enter: Run Cell
        if (e.ctrlKey && e.key === 'Enter') {
            e.preventDefault(); // Prevent default browser behavior (e.g., newline in textarea)
            e.stopPropagation(); // Stop event bubbling
            
            console.log('Ctrl+Enter detected!');
            
            // Find the currently focused CodeMirror editor
            const focused = document.activeElement.closest('.cm-editor');
            if (focused) {
                console.log('Found focused editor');
                
                // Find the parent cell container
                const cell = focused.closest('.code-cell');
                if (cell) {
                    console.log('Found parent cell');
                    
                    // Find and click the run button in this cell
                    const runBtn = cell.querySelector('.gutter-run-button');
                    if (runBtn) {
                        console.log('Clicking run button...');
                        runBtn.click();
                    } else {
                        console.log('No gutter run button found in cell for Ctrl+Enter.');
                    }
                }
            } else {
                console.log('No CodeMirror editor focused for Ctrl+Enter.');
            }
        } 
        // Shift+Enter: Run Cell (MODIFIED: removed "and Add New Cell")
        else if (e.shiftKey && e.key === 'Enter') {
            const focused = document.activeElement.closest('.cm-editor');
            if (focused) {
                e.preventDefault(); // Prevent default browser behavior
                e.stopPropagation(); // Stop event bubbling
                
                console.log('Shift+Enter detected - running cell'); // Updated log message
                
                const cell = focused.closest('.code-cell');
                if (cell) {
                    const runBtn = cell.querySelector('.gutter-run-button');
                    if (runBtn) {
                        runBtn.click();
                    }
                }
            }
        }
        // NEW: Ctrl+Down Arrow: Add new cell
        else if (e.ctrlKey && e.key === 'ArrowDown') {
            e.preventDefault(); // Prevent default browser behavior (e.g., scrolling)
            e.stopPropagation(); // Stop event bubbling
            
            console.log('Ctrl+Down Arrow detected!');
            
            const addBtn = document.getElementById('add-cell-button'); 
            if (addBtn) {
                console.log('Clicking Add Cell button...');
                addBtn.click();
            } else {
                console.warn('Add Cell button not found by ID. Ctrl+Down Arrow for new cell failed.');
            }
        }
    });
});

// --- File Tree Colorization (.dnb files) ---
function colorizeDnbFiles() {
    setTimeout(() => {
        document.querySelectorAll('.q-tree__node').forEach(node => {
            const labelElement = node.querySelector('.q-tree__node-header-content > div:last-child');
            if (labelElement && labelElement.textContent.endsWith('.dnb')) {
                const headerElement = node.querySelector('.q-tree__node-header');
                if (headerElement) {
                    headerElement.classList.add('dnb-file-node');
                }
            }
        });
    }, 100);
}

document.addEventListener('DOMContentLoaded', colorizeDnbFiles);

const treeObserver = new MutationObserver((mutations) => {
    let treeChanged = false;
    mutations.forEach((mutation) => {
        if (mutation.type === 'childList' && mutation.target.classList.contains('q-tree')) {
            treeChanged = true;
        }
        mutation.addedNodes.forEach((node) => {
            if (node.nodeType === 1 && 
                (node.classList?.contains('q-tree__node') || 
                 node.querySelector?.('.q-tree__node'))) {
                treeChanged = true;
            }
        });
    });
    if (treeChanged) {
        colorizeDnbFiles();
    }
});

treeObserver.observe(document.body, { childList: true, subtree: true });

// --- CodeMirror Auto-Expand ---
function setupAutoExpand() {
    setTimeout(() => {
        document.querySelectorAll('.cm-editor').forEach(editor => {
            if (editor._autoExpandSetup) return;
            
            const view = editor.CodeMirror;
            if (!view) return;
            
            function updateHeight() {
                const lineHeight = 24;
                const padding = 16;
                const minLines = 4;
                const maxLines = 25;
                
                const lineCount = Math.max(1, view.state.doc.lines);
                const targetLines = Math.max(minLines, Math.min(maxLines, lineCount + 1));
                const newHeight = (targetLines * lineHeight) + padding;
                
                editor.style.height = newHeight + 'px';
                
                setTimeout(() => {
                    if (view.requestMeasure) {
                        view.requestMeasure();
                    }
                }, 0);
            }
            
            if (view.updateListener) {
                const extension = view.updateListener.of((update) => {
                    if (update.docChanged || update.geometryChanged) {
                        setTimeout(updateHeight, 10);
                    }
                });
                view.dispatch({
                    effects: view.state.reconfigure.of([
                        ...view.state.extensions,
                        extension
                    ])
                });
            }
            
            updateHeight();
            editor._autoExpandSetup = true;
        });
    }, 100);
}

document.addEventListener('DOMContentLoaded', setupAutoExpand);

const observer = new MutationObserver((mutations) => {
    let hasNewEditor = false;
    mutations.forEach((mutation) => {
        mutation.addedNodes.forEach((node) => {
            if (node.nodeType === 1 && 
                (node.classList?.contains('cm-editor') || 
                 node.querySelector?.('.cm-editor'))) {
                hasNewEditor = true;
            }
        });
    });
    if (hasNewEditor) {
        setTimeout(setupAutoExpand, 100);
    }
});

observer.observe(document.body, { childList: true, subtree: true });
</script>
"""

def create_file_tree(path: Path = Path('.'), max_depth=3, current_depth=0) -> Tuple[List[Dict], List[Tuple[str, bool, float]]]:
    """
    Create a tree structure for file explorer and a snapshot of the directory state.
    Path should be a Path object.
    Returns (tree_data, state_snapshot).
    """
    tree_data: List[Dict] = []
    state_snapshot: List[Tuple[str, bool, float]] = [] 

    if current_depth >= max_depth:
        return tree_data, []

    try:
        if not path.exists() or not path.is_dir():
            logger.warning(f"Path does not exist or is not a directory: {path}")
            return tree_data, []

        items_to_process = []
        for item in path.iterdir():
            if item.name.startswith('.'): # Skip hidden files/directories
                continue
            try:
                items_to_process.append(item)
            except OSError as e:
                logger.warning(f"Could not access item {item}: {e}")
                continue
        
        items_to_process.sort(key=lambda x: (x.is_file(), x.name.lower()))

        for item in items_to_process:
            node = {
                'id': str(item.resolve()),
                'label': item.name,
                'icon': 'folder' if item.is_dir() else get_file_icon(item.suffix),
                'path': str(item.resolve()),
                'is_file': item.is_file() 
            }

            if item.is_dir() and current_depth < max_depth - 1:
                children, _ = create_file_tree(item, max_depth, current_depth + 1)
                if children:
                    node['children'] = children
            
            tree_data.append(node)
            state_snapshot.append((str(item.resolve()), item.is_file(), item.stat().st_mtime))
            
    except PermissionError:
        logger.warning(f"Permission denied for path: {path}")
    except Exception as e:
        logger.error(f"Error reading directory {path}: {e}")

    state_snapshot.sort(key=lambda x: (x[0], x[1], x[2]))
    return tree_data, state_snapshot

def get_file_icon(file_extension):
    """Return appropriate icon for file type"""
    icons = {
        '.py': 'code', '.sql': 'storage', '.csv': 'table_view', '.xlsx': 'table_view',
        '.xls': 'table_view', '.json': 'data_object', '.txt': 'description', '.md': 'article',
        '.html': 'web', '.css': 'palette', '.js': 'javascript', '.pdf': 'picture_as_pdf',
        '.png': 'image', '.jpg': 'image', '.jpeg': 'image', '.gif': 'image',
        '.dnb': 'edit_note' 
    }
    return icons.get(file_extension.lower(), 'description')

class NotebookApp:
    def __init__(self):
        self.cells = []
        self.dataframes = {}
        self.db_connection = None
        self.ssh_tunnel = None
        self.connection_config = {}
        self.last_successful_config = {}
        self.python_globals = {}
        self.is_dark_mode = True
        self.user_data_path = Path.home() / "DataNotebookRoot" 
        self.user_data_path.mkdir(parents=True, exist_ok=True) 
        self.app_config_dir = self.user_data_path / ".app_config" 
        self.app_config_dir.mkdir(parents=True, exist_ok=True)
        self.credentials_file = self.app_config_dir / 'credentials.json'
        self.working_directory: Path = self.user_data_path.resolve()
        self.current_filename = None
        self.is_modified = False
        self.last_tree_state: Optional[List[Tuple[str, bool, float]]] = None 

    def generate_cell_id(self):
        return str(uuid.uuid4())[:8]

    def save_credentials(self, config: Dict[str, Any]):
        try:
            self.credentials_file.parent.mkdir(parents=True, exist_ok=True)
            safe_config = config.copy()
            safe_config['password_saved'] = bool(config.get('db_password'))
            if 'db_password' in safe_config:
                del safe_config['db_password']
            with open(self.credentials_file, 'w') as f:
                json.dump(safe_config, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Failed to save credentials: {e}", exc_info=True)
            return False

    def load_credentials(self):
        try:
            if self.credentials_file.exists():
                with open(self.credentials_file, 'r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            logger.error(f"Failed to load credentials: {e}", exc_info=True)
            return {}

    def _has_ssh_config(self, config: Dict[str, Any]) -> bool:
        ssh_fields = ['ssh_host', 'ssh_username', 'ssh_private_key']
        return all(config.get(field, '').strip() for field in ssh_fields)

    async def connect_to_database(self, config: Dict[str, Any]):
        self.connection_config = config
        if self.db_connection:
            try: 
                self.db_connection.close()
            except Exception: 
                pass
            finally: 
                self.db_connection = None
        if self.ssh_tunnel:
            try: 
                self.ssh_tunnel.stop()
            except Exception: 
                pass
            finally: 
                self.ssh_tunnel = None

        try:
            use_ssh = self._has_ssh_config(config)
            if use_ssh:
                logger.info("SSH configuration detected. Establishing SSH tunnel...")
                self.ssh_tunnel = SSHTunnelForwarder(
                    (config['ssh_host'], int(config.get('ssh_port', 22))),
                    ssh_username=config['ssh_username'], 
                    ssh_pkey=config['ssh_private_key'],
                    remote_bind_address=(config['db_host'], int(config['db_port'])),
                    local_bind_address=('localhost', 6543))
                self.ssh_tunnel.start()
                logger.info("Connecting to database through SSH tunnel...")
                self.db_connection = psycopg2.connect(
                    host=self.ssh_tunnel.local_bind_host, 
                    port=self.ssh_tunnel.local_bind_port,
                    database=config['db_name'], 
                    user=config['db_user'], 
                    password=config['db_password'])
                logger.info("Database connection established via SSH tunnel")
            else:
                logger.info("No SSH configuration provided. Connecting directly to database...")
                self.db_connection = psycopg2.connect(
                    host=config['db_host'], 
                    port=int(config['db_port']),
                    database=config['db_name'], 
                    user=config['db_user'], 
                    password=config['db_password'])
                logger.info("Direct database connection established")

            self.last_successful_config = config.copy()
            return True, f"Connected successfully {'via SSH tunnel' if use_ssh else 'directly'}"

        except Exception as e:
            logger.error(f"Connection error: {e}", exc_info=True)
            if self.ssh_tunnel and hasattr(self.ssh_tunnel, 'is_active') and self.ssh_tunnel.is_active:
                try: 
                    self.ssh_tunnel.stop()
                except Exception: 
                    pass
            self.ssh_tunnel = None
            if self.db_connection:
                try: 
                    self.db_connection.close()
                except Exception: 
                    pass
                self.db_connection = None
            return False, str(e)

    def execute_sql(self, query: str, save_to_df: Optional[str] = None) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
        if not self.db_connection:
            return None, "Not connected to database", None
        try:
            df = pd.read_sql_query(query, self.db_connection)
            if save_to_df:
                self.dataframes[save_to_df] = df
                self.python_globals[save_to_df] = df
                return df, f"Query successful. DataFrame saved as '{save_to_df}'.", save_to_df
            return df, "Query successful.", None
        except Exception as e:
            logger.error(f"Query execution error: {e}", exc_info=True)
            return None, str(e), None

    def mark_modified(self):
        """Mark the notebook as modified"""
        self.is_modified = True
        if hasattr(self, 'title_label'):
            filename = self.current_filename or "Untitled"
            self.title_label.text = f"{filename}*"

    def mark_saved(self):
        """Mark the notebook as saved"""
        self.is_modified = False
        if hasattr(self, 'title_label'):
            filename = self.current_filename or "Untitled"
            self.title_label.text = f"{filename}"

    def serialize_notebook(self) -> Dict[str, Any]:
        """Serialize the current notebook state to a dictionary"""
        notebook_data = {
            'version': '1.0',
            'created_at': datetime.now().isoformat(),
            'working_directory': str(self.working_directory),
            'is_dark_mode': self.is_dark_mode,
            'cells': [],
            'connection_config': self.last_successful_config.copy() if self.last_successful_config else {}
        }
        
        if 'db_password' in notebook_data['connection_config']:
            del notebook_data['connection_config']['db_password']
        
        for cell_data in self.cells:
            cell_info = {
                'id': cell_data['id'],
                'type': cell_data['type'].value,
                'code': cell_data['code'].value,
                'df_name': cell_data['df_name'].value,
                'is_collapsed': cell_data['is_collapsed']()
            }
            notebook_data['cells'].append(cell_info)
        
        return notebook_data

    def save_notebook(self, filepath: str) -> bool:
        """Save the notebook to a file"""
        try:
            notebook_data = self.serialize_notebook()
            
            if not filepath.endswith('.dnb'):
                filepath += '.dnb'
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(notebook_data, f, indent=2, ensure_ascii=False)
            
            self.current_filename = Path(filepath).name
            self.mark_saved()
            logger.info(f"Notebook saved to: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save notebook: {e}", exc_info=True)
            return False

    async def load_notebook(self, filepath: str) -> bool:
        """Load a notebook from a file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                notebook_data = json.load(f)
            
            if 'version' not in notebook_data or 'cells' not in notebook_data:
                logger.error("Invalid notebook format")
                return False
            
            await self.clear_all_cells()
            
            if 'working_directory' in notebook_data:
                try:
                    new_wd = Path(notebook_data['working_directory'])
                    if new_wd.exists() and new_wd.is_dir():
                        await update_working_directory_and_tree(str(new_wd))
                except Exception as e:
                    logger.warning(f"Could not restore working directory: {e}")
            
            if 'is_dark_mode' in notebook_data:
                target_dark_mode = notebook.is_dark_mode # Current UI mode
                if target_dark_mode != notebook_data['is_dark_mode']: # Desired mode
                    toggle_dark_mode() # Call function to change UI and app state
            
            if 'connection_config' in notebook_data and notebook_data['connection_config']:
                self.last_successful_config = notebook_data['connection_config'].copy()
            
            for cell_info in notebook_data['cells']:
                await add_cell(cell_info['type'].lower())
                
                if self.cells:
                    cell_data = self.cells[-1]
                    cell_data['code'].set_value(cell_info.get('code', ''))
                    cell_data['df_name'].set_value(cell_info.get('df_name', ''))
                    
                    if cell_info.get('is_collapsed', False):
                        cell_data['toggle_collapse']()
            
            self.current_filename = Path(filepath).name
            self.mark_saved()
            logger.info(f"Notebook loaded from: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load notebook: {e}", exc_info=True)
            return False

    async def clear_all_cells(self):
        """Clear all cells from the notebook"""
        for cell_data in self.cells[:]:
            cell_data['container'].delete()
        
        self.cells.clear()
        self.dataframes.clear()
        self.python_globals.clear()

    async def new_notebook(self):
        """Create a new notebook (clear current state)"""
        await self.clear_all_cells()
        self.current_filename = None
        self.mark_saved()
        await add_cell('sql')

    async def execute_python(self, code: str) -> Tuple[bool, str, str]:
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()

        application_process_cwd = Path.cwd()
        
        # Initialize with plain text, will be updated to HTML if needed
        output_type = 'text/plain' 
        # Use a list to collect multiple parts of the result (e.g., stdout, dataframe, plot)
        final_result_representation_parts = []
        
        # Flag to indicate if a figure was handled by custom_display_func
        figure_explicitly_handled = False 

        try:
            user_working_dir = self.working_directory.resolve()
            if user_working_dir.is_dir():
                os.chdir(user_working_dir)
                logger.info(f"Changed CWD to: {user_working_dir} for Python execution.")
            else:
                logger.warning(f"User working directory '{user_working_dir}' is not a valid directory. "
                               f"Executing Python code in application CWD: '{application_process_cwd}'.")

            # IMPORTANT: Clear all existing figures before running user code
            # This prevents old plots from reappearing or being captured.
            plt.close('all') 

            # Ensure plt is available in the execution environment
            exec_globals = {'pd': pd, 'np': np, 'asyncio': asyncio, 'plt': plt, **self.python_globals}

            def custom_display_func(obj):
                nonlocal final_result_representation_parts, output_type, figure_explicitly_handled
                if isinstance(obj, pd.DataFrame):
                    final_result_representation_parts.append(obj.to_html(classes='dataframe', border=0, max_rows=20, escape=False))
                    output_type = 'text/html'
                elif isinstance(obj, matplotlib.figure.Figure): # NEW: Handle Matplotlib Figures
                    buffer = io.BytesIO()
                    # Save the figure to the in-memory buffer
                    obj.savefig(buffer, format='png', bbox_inches='tight', pad_inches=0.1) 
                    # bbox_inches='tight' and pad_inches=0.1 help ensure the plot fits well
                    
                    image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
                    
                    # Embed the base64 encoded image in an HTML <img> tag
                    final_result_representation_parts.append(
                        f'<img src="data:image/png;base64,{image_base64}" style="max-width: 100%; height: auto; display: block; margin: 10px 0;"/>'
                    )
                    output_type = 'text/html' # Output type becomes HTML if a plot is included
                    plt.close(obj) # Close the figure to free up memory
                    figure_explicitly_handled = True # Mark that a figure was processed

                elif obj is not None: # Default for other non-None objects
                    # Only add repr if it's not a displayable type already handled
                    # This prevents adding redundant repr for DataFrames/Figures
                    final_result_representation_parts.append(repr(obj))
                    # If only plain text is added here, ensure output_type reflects that unless already html
                    if output_type != 'text/html':
                         output_type = 'text/plain'

            exec_globals['display'] = custom_display_func

            modified_code = code.replace('time.sleep(', 'await asyncio.sleep(')
            needs_async = ('await ' in modified_code or 'async def' in code)

            if needs_async:
                exec_code = f"async def __async_exec():\n{chr(10).join('    ' + line for line in modified_code.split(chr(10)))}\n\n__async_result = __async_exec()"
                exec(exec_code, exec_globals, exec_globals)
                await exec_globals['__async_result']
            else:
                exec(code, exec_globals, exec_globals)

            self.python_globals.update({
                k: v for k, v in exec_globals.items()
                # Exclude keys that are part of the execution environment, not user-defined globals
                if not k.startswith('__') and k not in ['pd', 'np', 'display', 'asyncio', 'plt', 'matplotlib', 'io', 'base64']
            })

            std_out_content = captured_output.getvalue()

            # NEW: Post-execution check for figures created implicitly
            # This catches cases like `plt.plot(...)` where a figure is created but not explicitly
            # returned or passed to `display()`. `plt.show()` in user code would normally finalize it.
            # Since we use 'Agg' backend, plt.show() won't open a window, but we need to ensure
            # any created figures are captured.
            if plt.get_fignums() and not figure_explicitly_handled:
                for fig_num in plt.get_fignums():
                    fig = plt.figure(fig_num) # Get the figure object by its number
                    custom_display_func(fig) # Process this figure as well

            # Handle implicit display of last expression if not already handled
            # and if no figures were explicitly handled
            if not final_result_representation_parts and code.strip():
                lines = code.strip().split('\n')
                if lines:
                    last_line = lines[-1].strip()
                    is_simple_expression = (last_line and not any(
                        last_line.startswith(kw) for kw in
                        ['import ', 'from ', 'def ', 'class ', 'if ', 'for ', 'while ',
                         'with ', 'try ', 'print(', '#', '@']
                    ) and '=' not in last_line)

                    if is_simple_expression:
                        try:
                            evaluated_result = eval(last_line, exec_globals)
                            # Pass the result to custom_display_func to handle DataFrames/Figures
                            # If it's a simple type, it will be added as repr() by custom_display_func
                            custom_display_func(evaluated_result)
                        except Exception: 
                            pass # Ignore errors during eval if it's not a displayable object

            # Combine all output parts
            final_result_representation = "\n".join(final_result_representation_parts)

            combined_output = ""
            if std_out_content:
                # If there's stdout, always include it as pre-formatted text
                combined_output = f"<pre>{std_out_content.strip()}</pre>"
                # If there's also HTML output (DataFrame, plot), append it
                if final_result_representation and output_type == 'text/html':
                    combined_output += f"\n{final_result_representation}"
                # If there's only std_out and possibly plain text repr, combine them
                elif final_result_representation:
                    combined_output += f"\n<pre>{final_result_representation}</pre>"
                # Ensure output_type is HTML if any HTML content was added, else plain
                output_type = 'text/html' if (final_result_representation and output_type == 'text/html') or std_out_content else 'text/plain'
            elif final_result_representation:
                # If no stdout but there's a result (DF, plot, or repr)
                combined_output = final_result_representation
                # output_type should already be set correctly by custom_display_func
            
            if not combined_output.strip():
                combined_output = "Code executed successfully (no output)."
                output_type = 'text/plain'
            
            self.mark_modified()
            return True, combined_output, output_type

        except Exception as e:
            error_message = f"Error: {str(e)}\n{traceback.format_exc()}"
            std_out_content = captured_output.getvalue()
            if std_out_content: 
                error_message = f"{std_out_content}\n{error_message}"
            return False, error_message, 'text/plain'
        finally:
            sys.stdout = old_stdout
            os.chdir(application_process_cwd)
            # Ensure all figures are closed even if an error occurred
            plt.close('all') 
            logger.info(f"Restored CWD to: {application_process_cwd} after Python execution.")

notebook = NotebookApp()
ui.add_head_html(custom_css)

# Global UI component variables
left_drawer_instance: Optional[ui.left_drawer] = None
file_tree: Optional[ui.tree] = None
tree_container: Optional[ui.scroll_area] = None
cell_container: Optional[ui.column] = None
status_indicator: Optional[ui.html] = None
status_label: Optional[ui.label] = None
reconnect_btn: Optional[ui.button] = None
title_label: Optional[ui.label] = None
working_dir_input: Optional[ui.input] = None
current_working_dir_label: Optional[ui.label] = None
working_dir_display: Optional[ui.label] = None


async def pick_file_native(mode='save', file_types=None, initial_file: Optional[str] = None, initial_dir: Optional[str] = None) -> Optional[str]:
    """Opens an OS-native file picker dialog for save or open"""
    if not TKINTER_AVAILABLE:
        logger.error("tkinter is not available for native file picker.")
        ui.notify("Native file picker is not available (tkinter module missing).", type='warning') # Notify user
        return None

    # Default file_types if None is passed (e.g. for generic file operations not covered elsewhere)
    # This specific default is less likely to be hit if called from save_cell_code or handle_save_notebook
    if file_types is None:
        file_types = [("All Files", "*.*")]

    loop = asyncio.get_event_loop()

    def _select_file():
        root = tkinter.Tk()
        root.withdraw()
        root.attributes('-topmost', True)

        _initial_dir = initial_dir if initial_dir and Path(initial_dir).is_dir() else str(notebook.working_directory)

        if mode == 'save':
            _initial_file = initial_file if initial_file else "untitled"

            # --- MODIFICATION START: Dynamic defaultextension and title ---
            _derived_defaultextension = ""
            if file_types and isinstance(file_types, list) and len(file_types) > 0:
                first_file_type_pattern = file_types[0][1]
                if isinstance(first_file_type_pattern, str) and first_file_type_pattern.startswith("*."):
                    if len(first_file_type_pattern) > 2 and first_file_type_pattern != "*.*":
                        _derived_defaultextension = first_file_type_pattern[1:] # e.g., ".py", ".sql", ".dnb"

            # Fallback for defaultextension if not derived from file_types and initial_file has an extension
            if not _derived_defaultextension and _initial_file:
                try:
                    ext = Path(_initial_file).suffix
                    if ext and ext != ".": # Ensure there is a suffix and it's not just "."
                        _derived_defaultextension = ext
                except Exception:
                    pass # Ignore errors in deriving from initial_file

            _title = "Save File As" # Generic default title for saving
            if _initial_file and "_cell_" in _initial_file: # Heuristic for cell saving
                _title = "Save Cell Code As"
            elif file_types and isinstance(file_types, list) and len(file_types) > 0:
                 first_file_type_label = file_types[0][0] # e.g., "Data Notebook"
                 if "notebook" in first_file_type_label.lower():
                     _title = "Save Notebook As"
            # --- MODIFICATION END ---

            filepath = filedialog.asksaveasfilename(
                initialdir=_initial_dir,
                initialfile=_initial_file,
                title=_title, # Use dynamic title
                filetypes=file_types,
                defaultextension=_derived_defaultextension # Use dynamic default extension
            )
        else: # mode == 'open'
            _title = "Open File" # Generic default title for opening
            if file_types and isinstance(file_types, list) and len(file_types) > 0:
                 first_file_type_label = file_types[0][0]
                 if "notebook" in first_file_type_label.lower():
                     _title = "Open Notebook"
            
            filepath = filedialog.askopenfilename(
                initialdir=_initial_dir,
                title=_title, # Use dynamic title for open dialog too
                filetypes=file_types
            )

        root.destroy()
        return filepath

    try:
        filepath_result = await loop.run_in_executor(None, _select_file)
        return filepath_result
    except Exception as e:
        logger.error(f"Error in native file picker: {e}", exc_info=True)
        ui.notify(f"Could not open file picker: {e}", type='negative')
        return None

async def handle_save_notebook():
    """Handle saving the notebook"""
    if not TKINTER_AVAILABLE:
        ui.notify("Native file picker is not available (tkinter module missing).", type='warning')
        return

    # MODIFIED: Removed initial_file and initial_dir as they are specific to cell saving
    filepath = await pick_file_native(mode='save', file_types=[("Data Notebook", "*.dnb"), ("All Files", "*.*")])
    if filepath:
        success = notebook.save_notebook(filepath)
        if success:
            ui.notify(f"Notebook saved successfully!", type='positive')
            await update_working_directory_and_tree(str(notebook.working_directory)) # Trigger tree update
        else:
            ui.notify("Failed to save notebook", type='negative')

async def handle_load_notebook():
    """Handle loading a notebook"""
    if notebook.is_modified:
        with ui.dialog() as confirm_dialog:
            with ui.card():
                ui.label("You have unsaved changes. Loading a new notebook will discard them.")
                with ui.row().classes('w-full justify-end mt-4'):
                    ui.button('Cancel', on_click=confirm_dialog.close)
                    async def proceed_load():
                        confirm_dialog.close()
                        await _do_load_notebook()
                    ui.button('Load Anyway', on_click=proceed_load).classes('bg-orange-500')
        confirm_dialog.open()
    else:
        await _do_load_notebook()

async def _do_load_notebook():
    """Actually perform the notebook loading"""
    # MODIFIED: Removed initial_file and initial_dir as they are specific to cell saving
    filepath = await pick_file_native(mode='open', file_types=[("Data Notebook", "*.dnb"), ("All Files", "*.*")])
    if filepath:
        success = await notebook.load_notebook(filepath)
        if success:
            ui.notify(f"Notebook loaded successfully!", type='positive')
        else:
            ui.notify("Failed to load notebook", type='negative')

# NEW: handle_new_notebook() function (Consolidated and kept)
async def handle_new_notebook():
    """Handle creating a new notebook"""
    if notebook.is_modified:
        with ui.dialog() as confirm_dialog:
            with ui.card():
                ui.label("You have unsaved changes. Creating a new notebook will discard them.")
                with ui.row().classes('w-full justify-end mt-4'):
                    ui.button('Cancel', on_click=confirm_dialog.close)
                    async def proceed_new():
                        confirm_dialog.close()
                        await notebook.new_notebook()
                        ui.notify("New notebook created", type='positive')
                    ui.button('New Anyway', on_click=proceed_new).classes('bg-orange-500')
        confirm_dialog.open()
    else:
        await notebook.new_notebook()
        ui.notify("New notebook created", type='positive')

# NEW FEATURE: Function to save individual cell code
async def save_cell_code(cell_data: Dict[str, Any]):
    """Saves the code of a specific cell to a file in the working directory."""
    
    code_content = cell_data['code'].value
    cell_type = cell_data['type'].value.lower() # 'sql' or 'python'
    
    if not code_content.strip():
        ui.notify("Cell is empty. Nothing to save.", type='warning')
        return

    # Determine file extension
    if cell_type == 'sql':
        file_extension = '.sql'
    elif cell_type == 'python':
        file_extension = '.py'
    else:
        ui.notify(f"Unsupported cell type for saving: {cell_type}", type='negative')
        return

    # Generate a base for the filename
    base_name_stem = "untitled"
    if notebook.current_filename:
        base_name_stem = Path(notebook.current_filename).stem # Get filename without .dnb extension
    
    filename_stem = f"{base_name_stem}_cell_{cell_data['id']}"
    
    target_dir = notebook.working_directory
    
    # Handle potential filename conflicts by appending a number if needed
    counter = 0
    # Initial proposed filename (without number suffix yet)
    current_filename_suggestion = f"{filename_stem}{file_extension}"
    actual_filepath = target_dir / current_filename_suggestion
    
    # Loop if the file already exists, appending _1, _2, etc.
    while actual_filepath.exists():
        counter += 1
        current_filename_suggestion = f"{filename_stem}_{counter}{file_extension}"
        actual_filepath = target_dir / current_filename_suggestion

    try:
        logger.info(f"Attempting to write cell code to: {actual_filepath}")
        actual_filepath.write_text(code_content, encoding='utf-8')
        ui.notify(f"Cell code saved as '{actual_filepath.name}' in working directory.", type='positive')
        await refresh_file_tree_ui() # Refresh the file tree to show the new file
    except Exception as e:
        logger.error(f"Failed to save cell code to {actual_filepath}: {e}", exc_info=True)
        ui.notify(f"Failed to save cell code: {e}", type='negative')


async def refresh_file_tree_ui():
    """Refreshes the file tree in the UI if changes are detected."""
    global file_tree, tree_container
    
    if not file_tree or not tree_container:
        # UI components not yet initialized, skip refresh
        return

    # Create new tree nodes and get current state snapshot
    new_tree_nodes, new_state_snapshot = create_file_tree(path=notebook.working_directory, max_depth=3)

    # Compare with last known state
    if new_state_snapshot != notebook.last_tree_state:
        logger.info("File tree changed. Updating UI...")
        tree_container.clear()
        with tree_container:
            file_tree = ui.tree(
                new_tree_nodes, label_key='label', children_key='children', node_key='id',
            ).classes('w-full')

            # --- MODIFIED: Changed from 'click' to 'node_dblclick' for .dnb files ---
            def on_tree_double_click(event):
                if event.node.get('is_file') and event.node.get('path', '').endswith('.dnb'):
                    asyncio.create_task(load_notebook_from_path(event.node['path']))
            
            file_tree.on('node_dblclick', on_tree_double_click) # Attach to double click
            # --- END MODIFIED ---

            if new_tree_nodes:
                expand_ids = [node['id'] for node in new_tree_nodes if not node.get('is_file', True) and 'children' in node]
                if expand_ids: 
                    file_tree.expand(expand_ids)
            else:
                ui.label("Directory is empty or inaccessible.").classes('q-pa-md text-caption text-grey')
        
        # Update the last known state
        notebook.last_tree_state = new_state_snapshot
        
        # Trigger JavaScript to colorize .dnb files
        ui.run_javascript('colorizeDnbFiles()')

async def pick_directory_native() -> Optional[str]:
    """Opens an OS-native directory picker dialog."""
    if not TKINTER_AVAILABLE:
        logger.error("tkinter is not available for native directory picker.")
        return None

    loop = asyncio.get_event_loop()
    
    def _select_directory():
        root = tkinter.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        initial_dir = str(notebook.working_directory)
        directory = filedialog.askdirectory(initialdir=initial_dir, title="Select Working Directory")
        root.destroy()
        return directory

    try:
        directory_path = await loop.run_in_executor(None, _select_directory)
        return directory_path
    except Exception as e:
        logger.error(f"Error in native directory picker: {e}", exc_info=True)
        ui.notify(f"Could not open directory picker: {e}", type='negative')
        return None

async def handle_browse_working_directory():
    """Handles the click on the 'browse for working directory' button."""
    if not TKINTER_AVAILABLE:
        ui.notify("Native directory picker is not available (tkinter module missing or failed to import).", type='warning')
        return

    selected_path_str = await pick_directory_native()
    if selected_path_str:
        logger.info(f"Native directory picker returned: {selected_path_str}")
        if working_dir_input:
            working_dir_input.set_value(selected_path_str)
        # Call update_working_directory_and_tree, which now triggers refresh_file_tree_ui
        await update_working_directory_and_tree(selected_path_str)
    else:
        logger.info("Native directory picker cancelled or returned no path.")
        ui.notify("Directory selection cancelled or failed.", type='info')

async def update_working_directory_and_tree(new_path_str: str):
    global file_tree, working_dir_display

    if not new_path_str.strip():
        ui.notify("Working directory path cannot be empty.", type='warning')
        if working_dir_input: 
            working_dir_input.set_value(str(notebook.working_directory))
        return

    prospective_path = Path(new_path_str.strip())
    if not prospective_path.is_absolute():
        new_resolved_path = (notebook.working_directory / prospective_path).resolve()
    else:
        new_resolved_path = prospective_path.resolve()

    if new_resolved_path == notebook.working_directory:
        if working_dir_input: 
            working_dir_input.set_value(str(notebook.working_directory))
        await refresh_file_tree_ui() # Still refresh in case contents changed without path changing
        return

    if not new_resolved_path.is_dir():
        ui.notify(f"Invalid or inaccessible directory: {new_resolved_path}", type='negative')
        if working_dir_input: 
            working_dir_input.set_value(str(notebook.working_directory))
        return

    notebook.working_directory = new_resolved_path
    
    if working_dir_input:
        working_dir_input.set_value(str(notebook.working_directory))

    if working_dir_display:
        display_path = get_last_n_path_parts(str(notebook.working_directory), 2)
        working_dir_display.text = display_path

    # Force a refresh after changing directory
    await refresh_file_tree_ui() 

async def load_notebook_from_path(filepath: str):
    """Load a notebook from a specific file path"""
    if notebook.is_modified:
        with ui.dialog() as confirm_dialog:
            with ui.card():
                ui.label(f"You have unsaved changes. Loading '{Path(filepath).name}' will discard them.")
                with ui.row().classes('w-full justify-end mt-4'):
                    ui.button('Cancel', on_click=confirm_dialog.close)
                    async def proceed_load():
                        confirm_dialog.close()
                        success = await notebook.load_notebook(filepath)
                        if success:
                            ui.notify(f"Notebook '{Path(filepath).name}' loaded successfully!", type='positive')
                        else:
                            ui.notify("Failed to load notebook", type='negative')
                    ui.button('Load Anyway', on_click=proceed_load).classes('bg-orange-500')
        confirm_dialog.open()
    else:
        success = await notebook.load_notebook(filepath)
        if success:
            ui.notify(f"Notebook '{Path(filepath).name}' loaded successfully!", type='positive')
        else:
            ui.notify("Failed to load notebook", type='negative')

async def add_cell(cell_type='sql'):
    cell_id = notebook.generate_cell_id()
    with cell_container:
        cell_element = ui.column().classes('code-cell w-full cell-with-gutter')
        is_collapsed = False

        with cell_element:
            with ui.row().classes('code-cell-header w-full'):
                collapse_btn = ui.html('<button class="collapse-button"><span class="collapse-icon">🠉</span></button>')
                initial_select_value = 'Python' if cell_type == 'python' else cell_type.upper()
                cell_type_select = ui.select(options=['SQL', 'Python'], value=initial_select_value).classes('w-25 header-control-padding')
                df_name_input = ui.input(placeholder='  Save to Dataframe :', value='').classes('w-29 header-control-padding')
                df_name_input.visible = cell_type.upper() == 'SQL'
                cell_preview = ui.label('').classes('cell-preview')
                cell_preview.visible = False
                
                # NEW: Add save cell button
                ui.space()
                save_cell_btn = ui.button(icon='save_alt', color='primary').classes('save-button')
                
                delete_btn = ui.button('✖', color='red').classes('delete-button').props('round')
                # IMPORTANT: Adjust class for delete_btn to account for new save_cell_btn
                delete_btn.classes(remove='q-ml-auto') # Remove old margin if it conflicts with the new button
                delete_btn # Add a small margin to separate from save_cell_btn

            with ui.column().classes('code-cell-content w-full') as cell_content:
                cm_language = cell_type.lower()
                current_cm_theme = 'vscodeDark' if notebook.is_dark_mode else 'vscodeLight'
                code_editor = ui.codemirror(value='', language=cm_language, theme=current_cm_theme).classes('w-full code-editor')
                code_editor.props(f'data-cell-id="{cell_id}"')
                output_area = ui.markdown('').classes('output-area w-full')
                output_area.visible = False

            with ui.column().classes('cell-gutter') as cell_gutter:
                run_btn = ui.button('▶', color='primary').classes('gutter-run-button')
                with ui.column().classes('gutter-execution-status') as execution_status:
                    spinner = ui.spinner(size='xs', color='primary')
                    timer_label = ui.label('0s').classes('gutter-timer-text')
                execution_status.visible = False
                with ui.column().classes('gutter-execution-result') as execution_result:
                    result_icon = ui.label('').classes('gutter-result-icon')
                    result_time = ui.label('').classes('gutter-timer-text')
                execution_result.visible = False
        
        # Moved run_cell definition here, before it's used in on_click or cell_data
        async def run_cell():
            nonlocal is_collapsed # Declare nonlocal to modify is_collapsed
            if is_collapsed: 
                toggle_collapse()
            code = code_editor.value
            cell_type_val = cell_type_select.value
            logger.info(f"[{cell_id}] Run: {cell_type_val}, Code: {code[:50]!r}")
            if not code.strip():
                output_area.set_content('No code to execute.')
                output_area.visible = True
                return

            execution_status.visible = True
            execution_result.visible = False
            start_time = time.time()
            timer_active = True
            execution_success = False
            
            def update_timer():
                if timer_active: 
                    timer_label.text = f'{int(time.time() - start_time)}s'
            timer = ui.timer(0.1, update_timer)
            run_btn.disable()

            try:
                output_area.set_content('Running...')
                output_area.visible = True
                await asyncio.sleep(0.1)

                if cell_type_val == 'SQL':
                    df_name = df_name_input.value.strip()
                    result_df, message, saved_name = notebook.execute_sql(code, df_name or None)
                    if result_df is not None:
                        execution_success = True
                        output_text = f"Shape: {result_df.shape}\n\n{result_df.head(20).to_html(classes='dataframe', border=0, escape=False)}"
                        if len(result_df) > 20: 
                            output_text += f"\n\n*Showing first 20 of {len(result_df)} rows.*"
                        output_area.set_content(output_text)
                        notebook.mark_modified()
                    else:
                        execution_success = False
                        output_area.set_content(f"**SQL Error:** {message}")
                        ui.notify(f"Cell {cell_id}: SQL error.", type='negative')

                elif cell_type_val == 'Python':
                    success, py_output, py_output_type = await notebook.execute_python(code)
                    execution_success = success
                    if success:
                        output_area.set_content(py_output if py_output_type == 'text/html' else f"```\n{py_output}\n```")
                    else:
                        output_area.set_content(f"**Python Error:**\n```\n{py_output}\n```")
                        ui.notify(f"Cell {cell_id}: Python error.", type='negative')

            except Exception as e:
                execution_success = False
                logger.error(f"[{cell_id}] run_cell error: {e}", exc_info=True)
                output_area.set_content(f"**Unexpected Error:** {str(e)}\n{traceback.format_exc()}")
                ui.notify(f"Cell {cell_id}: Unexpected error.", type='error')

            finally:
                timer_active = False
                timer.cancel()
                final_time = time.time() - start_time
                execution_status.visible = False
                execution_result.visible = True
                result_icon.text = '✓' if execution_success else '☓'
                result_icon.classes('result-success' if execution_success else 'result-error',
                                    remove='result-error' if execution_success else 'result-success')
                result_time.text = f'{final_time:.2f}s' if final_time < 1 else f'{final_time:.1f}s'
                run_btn.enable()
            logger.info(f"[{cell_id}] End run_cell. Output visible: {output_area.visible}")

        def toggle_collapse():
            nonlocal is_collapsed
            is_collapsed = not is_collapsed
            if is_collapsed:
                cell_element.classes(add='collapsed')
                code_text = code_editor.value.strip()
                preview_text = (code_text.split('\n')[0][:50] + ('...' if len(code_text.split('\n')[0]) > 50 else '')) if code_text else 'Empty cell'
                cell_preview.text = preview_text
                cell_preview.visible = True
            else:
                cell_element.classes(remove='collapsed')
                cell_preview.visible = False
        collapse_btn.on('click', toggle_collapse)

        def on_cell_type_change():
            df_name_input.visible = cell_type_select.value == 'SQL'
            code_editor.language = cell_type_select.value.lower()
            notebook.mark_modified()
        cell_type_select.on_value_change(on_cell_type_change)

        def on_code_change():
            notebook.mark_modified()
        code_editor.on_value_change(on_code_change)

        def on_df_name_change():
            notebook.mark_modified()
        df_name_input.on_value_change(on_df_name_change)

    cell_data = {'id': cell_id, 'type': cell_type_select, 'code': code_editor,
                'output': output_area, 'df_name': df_name_input, 'container': cell_element,
                'execution_status': execution_status, 'timer_label': timer_label, 'spinner': spinner,
                'execution_result': execution_result, 'result_icon': result_icon, 'result_time': result_time,
                'is_collapsed': lambda: is_collapsed, 'toggle_collapse': toggle_collapse}
    notebook.cells.append(cell_data)

    def delete_cell():
        notebook.cells.remove(cell_data)
        cell_element.delete()
        notebook.mark_modified()
    
    run_btn.on_click(run_cell)
    delete_btn.on_click(delete_cell)
    # NEW: Attach save_cell_code to the save button
    save_cell_btn.on_click(functools.partial(save_cell_code, cell_data))
    
    # This ensures the add cell button is always at the bottom if it's part of the cell_container
    if hasattr(cell_container, '_add_cell_button'): 
        cell_container._add_cell_button.move(target_index=-1)

async def add_cell_and_mark_modified(cell_type='sql'):
    """Add a cell and mark notebook as modified"""
    await add_cell(cell_type)
    notebook.mark_modified()

async def setup_keyboard_shortcuts():
    """Setup keyboard shortcuts for save/load/new"""
    # Only register Python-handled shortcuts (Alt+S/O/N). Cell execution is handled by JS.
    ui.keyboard(on_key=handle_keyboard_shortcut)

def handle_keyboard_shortcut(event):
    """Handle keyboard shortcuts (Python side)"""
    if event.action.keydown: # Ensure it's a keydown event
        # Existing Alt shortcuts
        if event.modifiers.alt:
            if event.key.name == 's':
                asyncio.create_task(handle_save_notebook())
            elif event.key.name == 'o':
                asyncio.create_task(handle_load_notebook())
            elif event.key.name == 'n':
                asyncio.create_task(handle_new_notebook())
        # NEW: Ctrl+B to toggle left drawer
        elif event.modifiers.ctrl and event.key.name == 'b':
            if left_drawer_instance: # Check if the drawer object is initialized
                left_drawer_instance.toggle()
               


def toggle_dark_mode():
    """Toggle dark mode"""
    notebook.is_dark_mode = not notebook.is_dark_mode
    if notebook.is_dark_mode:
        ui.query('body').classes(add='dark-mode')
    else:
        ui.query('body').classes(remove='dark-mode')

    dark_mode_btn.props('icon=light_mode' if notebook.is_dark_mode else 'icon=dark_mode')

    new_theme = 'vscodeDark' if notebook.is_dark_mode else 'vscodeLight'
    for cell_data in notebook.cells:
        if hasattr(cell_data['code'], 'theme'):
            cell_data['code'].theme = new_theme
            
    notebook.mark_modified()

def get_last_n_path_parts(path, n=2):
    """Get the last n parts of a file path"""
    parts = Path(path).parts
    if len(parts) <= n:
        return str(path)
    return str(Path(*parts[-n:]))

# --- Main UI Layout ---
main_container = ui.element('div').classes('w-full main-container')
ui.query('body').classes(add='dark-mode')

with main_container:
    with ui.column().classes('w-full h-full'):
        # --- Toolbar ---
        with ui.row().classes('toolbar w-full'):
            ui.button(on_click=lambda: left_drawer_instance.toggle(), icon='menu').props('flat round color=primary').classes('drawer-button-hover-effect').style('margin-left: -6px; margin-top: -6px;')
            
            title_label = ui.label('Untitled').classes('text-2xl font-bold')
            notebook.title_label = title_label
                

            with ui.row().classes('notebook-controls ml-20'):
                ui.row()
                dark_mode_btn = ui.button(icon='light_mode', on_click=toggle_dark_mode).props('flat round').style('margin-top: -6px;')
                ui.row()
                ui.row()
                # FIX: Changed on_click from handle_save_notebook to handle_new_notebook
                ui.button('New', on_click=handle_new_notebook).classes('save-load-button').tooltip('New Notebook (Alt+N)').style('margin-top: 3px;')
                ui.button('Open', on_click=handle_load_notebook).classes('save-load-button').tooltip('Open Notebook (Alt+O)').style('margin-top: 3px;')
                ui.button('Save', on_click=handle_save_notebook).classes('save-load-button').tooltip('Save Notebook (Alt+S)').style('margin-top: 3px;')


            with ui.row().classes('connection-status'):
                async def handle_reconnect():
                    if not notebook.last_successful_config: 
                        ui.notify('No previous connection.', type='warning')
                        return
                    reconnect_btn.disable()
                    ui.notify('Reconnecting...', type='info')
                    success, message = await notebook.connect_to_database(notebook.last_successful_config)
                    status_indicator.content = f'<div class="status-indicator status_{"connected" if success else "disconnected"}"></div>'
                    status_label.text = "Connected" if success else "Disconnected"
                    ui.notify(f'Reconnection {"successful" if success else "failed"}: {message}', type='positive' if success else 'negative')
                    reconnect_btn.enable()
                
                reconnect_btn = ui.button(icon='refresh', on_click=handle_reconnect).props('flat round dense').tooltip('Reconnect')
                reconnect_btn.visible = False
                connect_btn = ui.button('Connect', on_click=lambda: connection_dialog.open()).classes('connect-button').tooltip('Configure DB Connection').style('margin-top: 3px;')
                status_indicator = ui.html('<div class="status-indicator status-disconnected"></div>')
                status_label = ui.label('Disconnected')

        cell_container = ui.column().classes('w-full cell-container')
        with cell_container:
            # Give the Add Cell button a specific ID for JavaScript to target
            cell_container._add_cell_button = ui.button('+ Add Cell', on_click=lambda: asyncio.create_task(add_cell_and_mark_modified('sql'))).classes('add-cell-button').props('id=add-cell-button')

# --- Left Drawer for File Explorer ---
with ui.left_drawer(value=False, elevated=False, top_corner=False, bordered=True) \
        .props('width=250 bordered behavior=desktop') \
        .classes('bg-[var(--bg-primary)]') as drawer:
    left_drawer_instance = drawer

    with ui.column().classes('w-full h-full no-wrap'):
        with ui.row().classes('items-center w-full gap-2'):
            browse_wd_button = ui.button(icon='folder_open', on_click=handle_browse_working_directory) \
                .classes('browse-wd-button-hover-effect') \
                .style('width: 28px; height: 28px; font-size: 12px; flex-shrink: 0').props('round')
            
            if not TKINTER_AVAILABLE:
                browse_wd_button.disable()
                browse_wd_button.tooltip("Native directory picker unavailable (tkinter missing)")
            
            display_path = get_last_n_path_parts(str(notebook.working_directory), 2) 
            working_dir_display = ui.label(display_path)
            

        # Section 2 
        with ui.scroll_area().classes('col q-pl-sm file-tree-container').style('margin-left: -24px') as tc_instance:
            tree_container = tc_instance
            # Initial tree load: Use the new function signature and initialize last_tree_state
            initial_tree_nodes, initial_state_snapshot = create_file_tree(path=notebook.working_directory, max_depth=3)
            notebook.last_tree_state = initial_state_snapshot # Set initial state
            
            file_tree = ui.tree(initial_tree_nodes, label_key='label', children_key='children', node_key='id').classes('w-full')

            # --- MODIFIED: Changed from 'click' to 'node_dblclick' for .dnb files ---
            def on_tree_double_click(event):
                if event.node.get('is_file') and event.node.get('path', '').endswith('.dnb'):
                    asyncio.create_task(load_notebook_from_path(event.node['path']))
            
            file_tree.on('node_dblclick', on_tree_double_click) # Attach to double click
            # REMOVED: file_tree.on('click', on_tree_click) was here.
            # --- END MODIFIED ---

            if initial_tree_nodes:
                expand_ids = [node['id'] for node in initial_tree_nodes if not node.get('is_file', True) and 'children' in node]
                if expand_ids: 
                    file_tree.expand(expand_ids)
            else:
                with tree_container: 
                    ui.label("Directory is empty or inaccessible.").classes('q-pa-md text-caption text-[var(--text-secondary)]')
            
            # Trigger JavaScript to colorize .dnb files after initial load
            ui.timer(0.2, lambda: ui.run_javascript('colorizeDnbFiles()'), once=True)

# --- Connection Dialog ---
with ui.dialog() as connection_dialog:
    with ui.card().classes('w-96'):
        saved_creds = notebook.load_credentials()
        ui.label('Database Configuration (Required)').classes('text-lg font-semibold mb-2')
        db_host = ui.input('Database Host', value=saved_creds.get('db_host', ''), placeholder='')
        db_port = ui.input('Database Port', value=saved_creds.get('db_port', '5439'), placeholder='')
        db_name = ui.input('Database Name', value=saved_creds.get('db_name', ''), placeholder='')
        db_user = ui.input('Database User', value=saved_creds.get('db_user', ''), placeholder='')
        db_password = ui.input('Database Password', placeholder='your_db_password').props('type=password')
        ui.separator().classes('my-4')
        with ui.expansion('SSH Configuration (Optional)', icon='vpn_key').classes('w-full'):
            ui.label('Configure SSH tunnel for secure database connections').classes('text-sm text-white-500 mb-2')
            ssh_host = ui.input('SSH Host', value=saved_creds.get('ssh_host', ''), placeholder='')
            ssh_port = ui.input('SSH Port', value=saved_creds.get('ssh_port', '22'), placeholder='')
            ssh_username = ui.input('SSH Username', value=saved_creds.get('ssh_username', ''), placeholder='')
            ssh_key_path = ui.input('SSH Private Key Path', value=saved_creds.get('ssh_private_key', ''), placeholder='')
        save_creds_checkbox = ui.checkbox('Save connection details (password excluded)', value=bool(saved_creds))
        with ui.row().classes('w-full justify-end mt-4'):
            ui.button('Cancel', on_click=connection_dialog.close)
            async def connect_action():
                config = {
                    'ssh_host': ssh_host.value.strip(), 
                    'ssh_port': ssh_port.value.strip() or '22',
                    'ssh_username': ssh_username.value.strip(), 
                    'ssh_private_key': ssh_key_path.value.strip(),
                    'db_host': db_host.value.strip(), 
                    'db_port': db_port.value.strip() or '5439',
                    'db_name': db_name.value.strip(), 
                    'db_user': db_user.value.strip(), 
                    'db_password': db_password.value
                }
                required_db = ['db_host', 'db_name', 'db_user', 'db_password']
                if any(not config[f] for f in required_db):
                    ui.notify(f'Fill required DB fields: {", ".join(required_db)}', type='warning')
                    return

                ssh_fields = ['ssh_host', 'ssh_username', 'ssh_private_key']
                if any(config[f] for f in ssh_fields) and not all(config[f] for f in ssh_fields):
                    ui.notify(f'Incomplete SSH. Missing: {", ".join(f for f in ssh_fields if not config[f])}. Will attempt direct.', type='warning')

                success, message = await notebook.connect_to_database(config)
                if success:
                    ui.notify('Connected!', type='positive')
                    status_indicator.content = '<div class="status-indicator status-connected"></div>'
                    status_label.text = 'Connected'
                    reconnect_btn.visible = True
                    if save_creds_checkbox.value: 
                        notebook.save_credentials(config)
                    connection_dialog.close()
                else: 
                    ui.notify(f'Connection failed: {message}', type='negative')
            ui.button('Connect', on_click=connect_action).classes('bg-blue-500')

async def initialize_app():
    await add_cell('sql')
    if notebook.cells and hasattr(notebook.cells[0]['code'], 'theme'):
        notebook.cells[0]['code'].theme = 'vscodeDark'
    
    await setup_keyboard_shortcuts()
    
    # Start the periodic file tree refresh
    ui.timer(0.5, refresh_file_tree_ui) # Refresh every 0.5 seconds

ui.timer(0.1, initialize_app, once=True) # Keep this to run initial setup

reload_dir = str(Path(__file__).resolve().parent)
app_source_dir = str(Path(__file__).resolve().parent)

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(
        title='Notebook', 
        port=8080, 
        native=False, 
        reload=False, 
        show=True, 
        favicon='🌐', 
        uvicorn_reload_dirs=None, 
        storage_secret="a_nice_secret_key_for_storage"
    )