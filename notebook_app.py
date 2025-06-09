import asyncio
from nicegui import ui, app
import json
import asyncpg
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
import io
import base64

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


# --- START: New JavaScript/CSS for DB Explorer ---
DB_EXPLORER_CSS = """
<style>
    /* Scoped CSS for the new DB Explorer */
    :root {
        --font-family-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
        --color-text-primary-local: #212529;
        --color-text-secondary-local: #6c757d;
        --color-text-muted-local: #868e96;
        --color-border-local: #dee2e6;
        --color-bg-hover-local: #f1f3f5;
        --color-schema-local: #0d6efd;
        --color-table-local: #198754;
        --color-column-local: #343a40;
        --color-pk-bg-local: #d1ecf1;
        --color-pk-text-local: #0c5460;
        --spacing-unit-local: 8px;
    }
    #db-explorer-container {
    font-family: var(--font-family, var(--font-family-sans));
    font-size: 14px;
    color: var(--text-primary, var(--color-text-primary-local));
    padding-left: 0px;  /* Changed from 10px to 0px */
    margin: 0;
    padding-top: 0;
    }
    /* Dark mode adaptations that leverage the app's existing theme */
    .dark-mode #db-explorer-container {
        --color-text-primary-local: #e0e0e0;
        --color-text-secondary-local: #b0b0b0;
        --color-text-muted-local: #888888;
        --color-border-local: #444444;
        --color-bg-hover-local: #2a2a2a;
        --color-schema-local: #58a6ff;
        --color-table-local: #7ee787;
        --color-column-local: #c9d1d9;
        --color-pk-bg-local: #388bfd26;
        --color-pk-text-local: #79c0ff;
    }
    #db-explorer-container .tree-node { position: relative; }
    #db-explorer-container .tree-node-content {
        margin-left: calc(var(--spacing-unit, var(--spacing-unit-local)) * 1);
        padding-left: calc(var(--spacing-unit, var(--spacing-unit-local)) * 1);
        border-left: 1px solid var(--border-color, var(--color-border-local));
    }
    #db-explorer-container .tree-node-header {
        display: flex;
        align-items: center;
        padding: calc(var(--spacing-unit, var(--spacing-unit-local)) / 2) 0;
        width: 100%;
        background: none; border: none;
        text-align: left; cursor: pointer;
        border-radius: 4px;
    }
    #db-explorer-container .tree-node-header:hover { background-color: var(--color-bg-hover, var(--color-bg-hover-local)); }
    #db-explorer-container .tree-node-header .icon-toggle {
        flex-shrink: 0; width: 16px; height: 16px;
        margin-right: var(--spacing-unit, var(--spacing-unit-local));
        color: var(--text-secondary, var(--color-text-secondary-local));
        transition: transform 0.2s ease-in-out;
    }
    #db-explorer-container .tree-node[aria-expanded="true"] > .tree-node-header .icon-toggle { transform: rotate(90deg); }
    #db-explorer-container .tree-node-label { font-weight: 500; cursor: pointer; }
    #db-explorer-container .tree-node-label:hover { text-decoration: underline; }
    #db-explorer-container .tree-node--schema > .tree-node-header .tree-node-label { color: var(--color-schema-local); }
    #db-explorer-container .tree-node--table > .tree-node-header .tree-node-label { color: var(--color-table-local); }
    #db-explorer-container .tree-node > .tree-node-content { display: none; }
    #db-explorer-container .tree-node[aria-expanded="true"] > .tree-node-content { display: block; }
    #db-explorer-container .column-item {
        display: flex; align-items: center;
        padding: calc(var(--spacing-unit, var(--spacing-unit-local)) / 4) 0;
        font-size: 13px; cursor: pointer;
        border-radius: 4px; transition: background-color 0.2s ease;
    }
    #db-explorer-container .column-item:hover { background-color: var(--color-bg-hover, var(--color-bg-hover-local)); }
    #db-explorer-container .column-name { 
        color: var(--color-column-local); 
        min-width: 100px;  /* Reduced from 150px */
        margin-right: 4px; /* Add small gap between name and type */
    }
    #db-explorer-container .column-type { 
        color: var(--color-text-muted, var(--color-text-muted-local)); 
        font-style: italic; 
        margin-right: var(--spacing-unit, var(--spacing-unit-local));
        flex-shrink: 0; /* Prevent the type from shrinking */
    }
    #db-explorer-container .pk-badge {
        background-color: var(--color-pk-bg, var(--color-pk-bg-local));
        color: var(--color-pk-text, var(--color-pk-text-local));
        padding: 2px 5px; border-radius: 3px;
        font-size: 10px; font-weight: 600;
    }
    #db-explorer-container .spinner {
        display: none; width: 16px; height: 16px;
        border: 2px solid currentColor; border-top-color: transparent;
        border-radius: 50%; animation: spin 0.8s linear infinite;
        margin-left: auto;
        margin-right: var(--spacing-unit, var(--spacing-unit-local));
        color: var(--text-secondary, var(--color-text-secondary-local));
    }
    #db-explorer-container .tree-node[aria-busy="true"] > .tree-node-header .spinner { display: inline-block; }
    #db-explorer-container .error-message { color: #dc3545; font-style: italic; padding: var(--spacing-unit, var(--spacing-unit-local)) 0; font-size: 13px; }
    @keyframes spin { to { transform: rotate(360deg); } }
</style>
"""

DB_EXPLORER_JS = """
<script>
// Make sure this script runs only once
if (!window.DbExplorer) {
    window.DbExplorer = class DbExplorer {
        constructor(elementId, initialData) {
            this.container = document.getElementById(elementId);
            if (!this.container) {
                console.error(`DbExplorer: Element with ID "${elementId}" not found.`);
                return;
            }
            this.initialData = initialData;
            this.container.addEventListener('click', this.handleTreeClick.bind(this));
            this.init();
        }
        init() {
            if (this.initialData && this.initialData.schemas) {
                this.renderSchemas(this.initialData.schemas);
            } else {
                this.renderError('Could not initialize: No schema data provided.');
            }
        }
        renderSchemas(schemas) {
            this.container.innerHTML = ''; // Clear previous content
            if (schemas.length === 0) {
                this.container.innerHTML = '<i>No schemas found in this database.</i>'
                return;
            }
            schemas.forEach(schema => {
                const schemaNode = this.createNode('schema', schema, { schema });
                this.container.appendChild(schemaNode);
            });
        }
        async handleTreeClick(event) {
            const header = event.target.closest('.tree-node-header');
            const columnItem = event.target.closest('.column-item');

            if (columnItem) {
                event.preventDefault();
                event.stopPropagation();
                const columnName = columnItem.querySelector('.column-name').textContent;
                await this.copyToClipboard(columnName, 'Column name');
                return;
            }
            if (!header) return;

            const labelElement = event.target.closest('.tree-node-label');
            if (labelElement) {
                event.preventDefault();
                event.stopPropagation();
                const node = header.parentElement;
                const nodeType = node.dataset.type;
                let textToCopy = '';
                let itemType = '';
                if (nodeType === 'schema') {
                    textToCopy = node.dataset.schema;
                    itemType = 'Schema name';
                } else if (nodeType === 'table') {
                    textToCopy = node.dataset.table;
                    itemType = 'Table name';
                }
                if (textToCopy) {
                    await this.copyToClipboard(textToCopy, itemType);
                    return;
                }
            }
            event.preventDefault();
            event.stopPropagation();

            const node = header.parentElement;
            const nodeType = node.dataset.type;
            const isExpanded = node.getAttribute('aria-expanded') === 'true';
            const isBusy = node.getAttribute('aria-busy') === 'true';
            if (isBusy) return;
            if (isExpanded) {
                this.collapseNode(node);
            } else {
                await this.expandNode(node, nodeType);
            }
        }
        async expandNode(node, nodeType) {
            if (node.querySelector('.tree-node-content')) {
                node.setAttribute('aria-expanded', 'true');
                return;
            }
            node.setAttribute('aria-busy', 'true');
            node.setAttribute('aria-expanded', 'true');
            try {
                let content;
                if (nodeType === 'schema') {
                    content = await this.loadTables(node.dataset.schema);
                } else if (nodeType === 'table') {
                    content = await this.loadColumns(node.dataset.schema, node.dataset.table);
                }
                if (content) {
                    const oldError = node.querySelector('.error-message-wrapper');
                    if(oldError) oldError.remove();
                    node.appendChild(content);
                }
            } catch (err) {
                console.error('Error expanding node:', err);
                node.setAttribute('aria-expanded', 'false');
                const errorWrapper = document.createElement('div');
                errorWrapper.className = 'tree-node-content error-message-wrapper';
                errorWrapper.innerHTML = `<div class="error-message">Failed to load: ${err.message}</div>`;
                node.appendChild(errorWrapper);
                node.setAttribute('aria-expanded', 'true');
            } finally {
                node.setAttribute('aria-busy', 'false');
            }
        }
        collapseNode(node) {
            node.setAttribute('aria-expanded', 'false');
        }
        async fetchApi(url) {
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            const result = await response.json();
            if (!result.success) {
                throw new Error(result.error || 'Unknown API error');
            }
            return result;
        }
        async loadTables(schema) {
            const result = await this.fetchApi(`/api/schema/tables/${encodeURIComponent(schema)}`);
            const contentDiv = document.createElement('div');
            contentDiv.className = 'tree-node-content';
            if (result.tables && result.tables.length > 0) {
                result.tables.forEach(table => {
                    const tableNode = this.createNode('table', table, { schema, table });
                    contentDiv.appendChild(tableNode);
                });
            } else {
                contentDiv.innerHTML = '<i style="padding: 4px 0; display: block; color: var(--text-secondary, #6c757d);">No tables found.</i>';
            }
            return contentDiv;
        }
        async loadColumns(schema, table) {
            const result = await this.fetchApi(`/api/schema/columns/${encodeURIComponent(schema)}/${encodeURIComponent(table)}`);
            const contentDiv = document.createElement('div');
            contentDiv.className = 'tree-node-content';
            if (result.columns && result.columns.length > 0) {
                result.columns.forEach(([name, type, isPrimaryKey]) => {
                    const normalizedType = this.normalizeDataType(type);
                    const pkBadge = isPrimaryKey === 'YES' ? `<span class="pk-badge">PK</span>` : '';
                    const columnEl = document.createElement('div');
                    columnEl.className = 'column-item';
                    columnEl.title = `Click to copy column name: ${this.escapeHtml(name)}`;
                    columnEl.innerHTML = `
                        <span class="column-name">${this.escapeHtml(name)}</span>
                        <span class="column-type">${this.escapeHtml(normalizedType)}</span>
                        ${pkBadge}
                    `;
                    contentDiv.appendChild(columnEl);
                });
            } else {
                contentDiv.innerHTML = '<i style="padding: 4px 0; display: block; color: var(--text-secondary, #6c757d);">No columns found.</i>';
            }
            return contentDiv;
        }
        createNode(type, label, data) {
            const node = document.createElement('div');
            node.className = `tree-node tree-node--${type}`;
            node.dataset.type = type;
            for (const key in data) { node.dataset[key] = data[key]; }
            node.setAttribute('aria-expanded', 'false');
            node.setAttribute('aria-busy', 'false');
            node.innerHTML = `
                <button class="tree-node-header">
                    <svg class="icon-toggle" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"></polyline></svg>
                    <span class="tree-node-label" title="Click to copy ${type} name: ${this.escapeHtml(label)}">${this.escapeHtml(label)}</span>
                    <div class="spinner"></div>
                </button>
            `;
            return node;
        }
        escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        normalizeDataType(pgType) {
            if (!pgType) return '';
            const typeMap = {
                'character varying': 'varchar',
                'timestamp without time zone': 'timestamp',
                'timestamp with time zone': 'timestamptz'
            };
            return typeMap[pgType.toLowerCase()] || pgType;
        }
        async copyToClipboard(text, itemType) {
            if (window.NiceGUI) {
                NiceGUI.py_emit('js_notify', `${itemType} "${text}" copied to clipboard!`, 'positive');
            }
            try {
                await navigator.clipboard.writeText(text);
            } catch (err) {
                console.error('Clipboard write failed:', err);
                if (window.NiceGUI) {
                    NiceGUI.py_emit('js_notify', 'Failed to copy to clipboard.', 'negative');
                }
            }
        }
        renderError(message) {
            this.container.innerHTML = `<div class="error-message" style="padding: 16px;">${this.escapeHtml(message)}</div>`;
        }
    }
}
</script>
"""
# --- END: New JavaScript/CSS for DB Explorer ---


# Custom CSS
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

html::-webkit-scrollbar,
.output-area-content::-webkit-scrollbar,
.cm-scroller::-webkit-scrollbar, /* CodeMirror editor scroll */
.file-tree-container::-webkit-scrollbar, /* Left drawer file tree */
.q-scrollarea__content::-webkit-scrollbar, /* Generic Quasar/NiceGUI scroll area */
div[class*="q-drawer"]::-webkit-scrollbar, /* For drawers if they scroll internally */
textarea::-webkit-scrollbar /* For textareas if they scroll */
{
    width: 10px;  /* Width of vertical scrollbar */
    height: 10px; /* Height of horizontal scrollbar */
}

html::-webkit-scrollbar-track,
.output-area-content::-webkit-scrollbar-track,
.cm-scroller::-webkit-scrollbar-track,
.file-tree-container::-webkit-scrollbar-track,
.q-scrollarea__content::-webkit-scrollbar-track,
div[class*="q-drawer"]::-webkit-scrollbar-track,
textarea::-webkit-scrollbar-track
{
    background: var(--bg-secondary); /* Track color, slightly offset from main bg */
    border-radius: 5px;
}

html::-webkit-scrollbar-thumb,
.output-area-content::-webkit-scrollbar-thumb,
.cm-scroller::-webkit-scrollbar-thumb,
.file-tree-container::-webkit-scrollbar-thumb,
.q-scrollarea__content::-webkit-scrollbar-thumb,
div[class*="q-drawer"]::-webkit-scrollbar-thumb,
textarea::-webkit-scrollbar-thumb
{
    background-color: var(--text-secondary); /* Thumb color */
    border-radius: 5px;
    border: 2px solid var(--bg-secondary); /* Creates a small border around the thumb, matching the track */
}

html::-webkit-scrollbar-thumb:hover,
.output-area-content::-webkit-scrollbar-thumb:hover,
.cm-scroller::-webkit-scrollbar-thumb:hover,
.file-tree-container::-webkit-scrollbar-thumb:hover,
.q-scrollarea__content::-webkit-scrollbar-thumb:hover,
div[class*="q-drawer"]::-webkit-scrollbar-thumb:hover,
textarea::-webkit-scrollbar-thumb:hover
{
    background-color: var(--text-primary); /* Thumb color on hover */
}

html::-webkit-scrollbar-corner,
.output-area-content::-webkit-scrollbar-corner,
.cm-scroller::-webkit-scrollbar-corner,
.file-tree-container::-webkit-scrollbar-corner,
.q-scrollarea__content::-webkit-scrollbar-corner,
div[class*="q-drawer"]::-webkit-scrollbar-corner,
textarea::-webkit-scrollbar-corner {
    background: var(--bg-secondary); /* Color of the bottom-right corner where scrollbars meet */
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
    --bg-primary: #181818;
    --bg-secondary: #181818;
    --bg-tertiary: #242424;
    --bg-output: #1a1a1a;
    --text-primary: #ffffff;
    --text-secondary: #e0e0e0;
    --border-color:  #444444;
    --input-bg: #242424;
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
    border-top-left-radius: 16px;
    border-top-right-radius: 16px;
    margin-bottom: 8px;
    background-color: var(--bg-tertiary);
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
    border-top-left-radius: 20px;
    border-top-right-radius: 16px;
    align-items: center;
    background-color: var(--bg-tertiary);
    border-bottom: 1px solid #fff8e6;
    padding: 8px 12px;
}

.code-cell.collapsed .code-cell-header {
    border-bottom: none;
    border-bottom-left-radius: 16px;
    border-bottom-right-radius: 16px;
}

.code-cell-content {
    transition: all 0.3s ease;
    overflow: hidden;
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

/* MODIFIED: Renamed .output-area to .output-container */
.output-container {
    border-bottom-left-radius: 16px;
    border-bottom-right-radius: 16px;
    background-color: var(--bg-primary);
    border-top: 1px solid var(--border-color); /* Separates output from editor */
    overflow: hidden; /* To ensure children respect rounded corners */
}

/* NEW: Styles for the content part of the output */
.output-area-content {
    font-family: monospace;
    font-size: 14px;
    max-height: 700px;
    overflow-y: auto;
    color: var(--text-primary);
    padding: 12px; /* Padding for the content itself */
}

/* MODIFIED: For child div if markdown creates one */
.output-area-content > div {
    overflow-x: auto;
}


.q-tabs {
    background-color: var(--bg-tertiary) !important;
    color: var(--text-primary) !important;
    border-bottom: 1px solid var(--border-color) !important;
}

.q-tabs {
    background-color: var(--bg-tertiary) !important;
    color: var(--text-primary) !important;
    border-bottom: 1px solid var(--border-color) !important;
}

.q-tab {
    color: var(--text-secondary) !important;
    min-height: 36px !important;
}

.q-tab--active {
    color: var(--text-primary) !important;
}

.q-tab__indicator {
    background-color: #5898D4 !important;
}

.dark-mode .q-tabs {
    background-color: var(--bg-tertiary) !important;
    border-bottom-color: var(--border-color) !important;
}

.dark-mode .q-tab {
    color: var(--text-secondary) !important;
}

.dark-mode .q-tab--active {
    color: var(--text-primary) !important;
}

.q-tab-panels {
    background-color: transparent !important;
}

.q-tab-panel {
    padding: 0 !important;
    margin: 0 !important;
}

.q-tab-panel .q-scroll-area {
    margin-top: 0 !important;
    padding-top: 0 !important;
}

.small-tab-label .q-tab__label {
    font-size: 0.7rem !important; /* Adjust as needed */
}

.q-tab-panel > .nicegui-column > :first-child {
    margin-top: 0 !important;
    padding-top: 0 !important;
}

/* Schema tree styling for dark mode */
.dark-mode .q-tree__node-header-content span {
    color: var(--text-primary)  ;
}

.dark-mode .q-tree__node-header-content span[style*="font-family: monospace"] > span {
    color: #9e9e9e !important;
    font-size: 0.8rem !important;
}

.dark-mode .q-tree__node-header-content span[style*="font-family: monospace"] > span:first-child {
    color: #FFFFFF !important;
    font-size: 0.8rem !important;
}

.dark-mode .q-tree__node-header-content span[style*="font-family: monospace"] > span:last-child {
    color: #9e9e9e !important;
    font-size: 0.8rem !important;
}

.dark-mode .q-tree__icon {
    color: var(--text-primary) !important;
}

.q-tree__node--child::before {
    display: none !important;
}

/* Ensure monospace column text is visible in dark mode */
.dark-mode span[style*="font-family: monospace"] span {
    color: var(--text-primary) !important;
}

/* Grey color for column types in dark mode */
.dark-mode span[style*="color: #9e9e9e"] {
    color: #9e9e9e !important;
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
    margin-right: auto;
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
    margin-rght: auto;
}

.save-button:hover {
    transform: scale(1.1);
    box-shadow: 0 4px 12px rgba(0,0,0,0.25);
}


.toolbar {
    display: flex;
    gap: 6px;
    padding: 0px 10px;
    height: 45px;
    background-color: var(--bg-tertiary);
    border-bottom: 1px solid var(--border-color);
    color: var(--text-primary);
    align-items: center;
    position: sticky;
    top: 0;
    z-index: 1000;
    width: 100%;
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

.code-editor {
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    font-size: 16px;
    line-height: 1.5;
    background-color: var(--input-bg) !important;
    color: var(--text-primary) !important;
    min-height: 100px !important;
    height: auto !important;
    resize: none;
}

.cm-editor {
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

.dark-mode .q-field__label {
    color: #bdbdbd !important; /* White labels in Dark Mode */
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

.dark-mode .cm-editor {
    background-color: var(--input-bg);
    color: var(--text-primary);
}

.dark-mode .cm-gutters {
    background-color: var(--bg-tertiary);
    color: var(--text-secondary);
    border-right: 1px solid var(--border-color);
    border-bottom-left-radius: 16px;
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
    top: 8px;
    width: 26px;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 4px;
    z-index: 10;
    transition: all 0.3s ease;
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
    transition: all 0.2s ease;
}

.drawer-button-hover-effect:hover {
    transform: scale(1.1);
}

.browse-wd-button-hover-effect {
    transition: all 0.2s ease;
}

.browse-wd-button-hover-effect:hover {
    transform: scale(1.1);
    box-shadow: 0 4px 12px rgba(0,0,0,0.25);
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
    font-size: 14px;
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
    border-left: 1px solid var(--border-color) !important;
    color: var(--text-primary) !important;
}


.dark-mode .q-tree {
    color: var(--text-primary) !important;
}

.dark-mode .q-tree .q-tree__node-header-content > div {
    color: var(--text-primary) !important;
}

.dark-mode .q-tree .q-tree__arrow,
.dark-mode .q-tree .q-tree__icon {
    color: var(--text-primary) !important;
}

.dark-mode .q-tree__node:before,
.dark-mode .q-tree__node:after {
    border-color: var(--text-secondary) !important;
}

.dark-mode .q-tree .q-tree__node-header:hover {
    background-color: var(--bg-output) !important;
}

.dark-mode .q-tree .q-tree__node-header:hover .dnb-file-node .q-icon,
.dark-mode .q-tree .q-tree__node-header:hover .dnb-file-node .q-tree__node-header-content > div,
.dark-mode .q-tree .q-tree__node-header:hover .q-tree__arrow {
    color: #ff8c00 !important;
}

.q-drawer .nicegui-column {
    height: 100%;
}

.notebook-controls {
    display: flex;
    gap: 8px;
    align-items: center;
}

.save-load-button {
    font-size: 12px !important;
    padding: 3px 1px 1px 1px !important;
    min-height: 25px !important;
    height: 25px !important;
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
    padding: 3px 1px 1px 1px !important;
    min-height: 25px !important;
    height: 25px !important;
    min-width: 78px !important;
    width:78px !important;
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

.dnb-file-node .q-icon,
.dnb-file-node .q-tree__node-header-content > div {
    color: orange !important;
}

.dark-mode .dnb-file-node .q-icon,
.dark-mode .dnb-file-node .q-tree__node-header-content > div {
    color: orange !important;
}

.q-tree__node-header:hover .dnb-file-node .q-icon,
.q-tree__node-header:hover .dnb-file-node .q-tree__node-header-content > div {
    color: #ff8c00 !important;
}

</style>


<script>
// --- Keyboard Shortcut Handling (Ctrl+Enter, Shift+Enter, Ctrl+Down Arrow) ---
document.addEventListener('DOMContentLoaded', function() {
    console.log('Setting up keyboard shortcuts...');

    document.addEventListener('keydown', function(e) {
        // Ctrl+Enter: Run Cell
        if (e.ctrlKey && e.key === 'Enter') {
            e.preventDefault();
            e.stopPropagation();

            console.log('Ctrl+Enter detected!');

            const focused = document.activeElement.closest('.cm-editor');
            if (focused) {
                console.log('Found focused editor');
                const cell = focused.closest('.code-cell');
                if (cell) {
                    console.log('Found parent cell');
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
        // Shift+Enter: Run Cell
        else if (e.shiftKey && e.key === 'Enter') {
            const focused = document.activeElement.closest('.cm-editor');
            if (focused) {
                e.preventDefault();
                e.stopPropagation();

                console.log('Shift+Enter detected - running cell');

                const cell = focused.closest('.code-cell');
                if (cell) {
                    const runBtn = cell.querySelector('.gutter-run-button');
                    if (runBtn) {
                        runBtn.click();
                    }
                }
            }
        }
        // Ctrl+Down Arrow: Add new cell
        else if (e.ctrlKey && e.key === 'ArrowDown') {
            e.preventDefault();
            e.stopPropagation();

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
            if item.name.startswith('.'):
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
    icons = {
        '.py': 'code', '.sql': 'storage', '.csv': 'view_list', '.xlsx': 'table_view',
        '.xls': 'table_view', '.json': 'data_object', '.txt': 'description', '.md': 'article',
        '.html': 'web', '.css': 'palette', '.js': 'javascript', '.pdf': 'picture_as_pdf',
        '.png': 'image', '.jpg': 'image', '.jpeg': 'image', '.gif': 'image',
        '.dnb': 'edit_note'
    }
    return icons.get(file_extension.lower(), 'description')

async def get_all_schema_data_optimized():
    """Fetches all schema, table, and column info in a single, efficient query."""
    if not notebook.db_connection:
        return {"error": "Not connected to database"}
    try:
        rows = await notebook.db_connection.fetch("""
            SELECT
                c.table_schema,
                c.table_name,
                c.column_name,
                c.data_type,
                CASE
                    WHEN pk.constraint_type = 'PRIMARY KEY' THEN 'YES'
                    ELSE 'NO'
                END AS is_primary_key
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT kcu.table_schema, kcu.table_name, kcu.column_name, tc.constraint_type
                FROM information_schema.key_column_usage AS kcu
                JOIN information_schema.table_constraints AS tc
                    ON kcu.constraint_name = tc.constraint_name
                    AND kcu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
            ) pk ON c.table_schema = pk.table_schema
                 AND c.table_name = pk.table_name
                 AND c.column_name = pk.column_name
            WHERE c.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY c.table_schema, c.table_name, c.ordinal_position
        """)
        schema_data = {}
        for row in rows:
            schema, table, column, dtype, is_pk = row['table_schema'], row['table_name'], row['column_name'], row['data_type'], row['is_primary_key']
            if schema not in schema_data:
                schema_data[schema] = {}
            if table not in schema_data[schema]:
                schema_data[schema][table] = []
            schema_data[schema][table].append((column, dtype, is_pk))
        return schema_data
    except Exception as e:
        logger.error(f"Error fetching database schema: {e}", exc_info=True)
        return {"error": f"Failed to fetch schema: {e}"}

# --- API Endpoints for the new JavaScript DB Explorer ---
@app.get('/api/schema/tables/{schema}')
async def get_tables_for_schema_api(schema: str):
    if not notebook.db_connection or not notebook.db_schema_data:
        return {'success': False, 'error': 'Not connected or schema not loaded.'}
    tables = sorted(list(notebook.db_schema_data.get(schema, {}).keys()))
    return {'success': True, 'tables': tables}

@app.get('/api/schema/columns/{schema}/{table}')
async def get_columns_for_table_api(schema: str, table: str):
    if not notebook.db_connection or not notebook.db_schema_data:
        return {'success': False, 'error': 'Not connected or schema not loaded.'}
    columns = notebook.db_schema_data.get(schema, {}).get(table, [])
    return {'success': True, 'columns': columns}


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
        self.db_schema_data: Dict[str, Any] = {} # Cache for the new DB explorer

    def generate_cell_id(self):
        return str(uuid.uuid4())[:8]

    def save_credentials(self, config: Dict[str, Any]):
        try:
            self.credentials_file.parent.mkdir(parents=True, exist_ok=True)
            # Retain password for saving
            with open(self.credentials_file, 'w') as f:
                json.dump(config, f, indent=2)
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
                await self.db_connection.close()  # asyncpg close is async
            except Exception:
                pass
            finally:
                self.db_connection = None
        if self.ssh_tunnel:
            try:
                await asyncio.to_thread(self.ssh_tunnel.stop)
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

                await asyncio.to_thread(self.ssh_tunnel.start)

                logger.info("Connecting to database through SSH tunnel...")
                # asyncpg connection - note the different parameter names
                self.db_connection = await asyncpg.connect(
                    host=self.ssh_tunnel.local_bind_host,
                    port=self.ssh_tunnel.local_bind_port,
                    database=config['db_name'],
                    user=config['db_user'],
                    password=config['db_password'])
                logger.info("Database connection established via SSH tunnel")
            else:
                logger.info("No SSH configuration provided. Connecting directly to database...")
                # asyncpg connection
                self.db_connection = await asyncpg.connect(
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
                    await asyncio.to_thread(self.ssh_tunnel.stop)
                except Exception:
                    pass
            self.ssh_tunnel = None
            if self.db_connection:
                try:
                    await self.db_connection.close()  # asyncpg close is async
                except Exception:
                    pass
                self.db_connection = None
            return False, str(e)

    async def execute_sql(self, query: str, save_to_df: Optional[str] = None) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
        if not self.db_connection:
            return None, "Not connected to database", None
        try:
            # asyncpg requires different approach - fetch records then convert to DataFrame
            records = await self.db_connection.fetch(query)

            if records:
                # Convert asyncpg records to DataFrame
                # Get column names from the first record
                columns = list(records[0].keys())
                # Convert records to list of tuples
                data = [tuple(record.values()) for record in records]
                df = pd.DataFrame(data, columns=columns)
            else:
                # Handle empty result set
                df = pd.DataFrame()

            if save_to_df:
                self.dataframes[save_to_df] = df
                self.python_globals[save_to_df] = df
                return df, f"Query successful. DataFrame saved as '{save_to_df}'.", save_to_df
            return df, "Query successful.", None
        except Exception as e:
            logger.error(f"Query execution error: {e}", exc_info=True)
            return None, str(e), None

    def mark_modified(self):
        self.is_modified = True
        if hasattr(self, 'title_label'):
            filename = self.current_filename or "Untitled"
            self.title_label.text = f"{filename}*"

    def mark_saved(self):
        self.is_modified = False
        if hasattr(self, 'title_label'):
            filename = self.current_filename or "Untitled"
            self.title_label.text = f"{filename}"

    def serialize_notebook(self) -> Dict[str, Any]:
        notebook_data = {
            'version': '1.0',
            'created_at': datetime.now().isoformat(),
            'working_directory': str(self.working_directory),
            'is_dark_mode': self.is_dark_mode,
            'cells': [],
            'connection_config': self.last_successful_config.copy() if self.last_successful_config else {}
        }

        # Keep password in connection_config when saving notebook
        # if 'db_password' in notebook_data['connection_config']:
        #     del notebook_data['connection_config']['db_password']

        for cell_data in self.cells:
            cell_info = {
                'id': cell_data['id'],
                'type': cell_data['type'].value,
                'code': cell_data['code'].value,
                'df_name': cell_data['df_name'].value,
                'is_collapsed': cell_data['is_collapsed'](),
                'show_all_rows': cell_data['show_all_rows']
            }
            notebook_data['cells'].append(cell_info)

        return notebook_data

    def save_notebook(self, filepath: str) -> bool:
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
                target_dark_mode = notebook.is_dark_mode
                if target_dark_mode != notebook_data['is_dark_mode']:
                    toggle_dark_mode()

            if 'connection_config' in notebook_data and notebook_data['connection_config']:
                self.last_successful_config = notebook_data['connection_config'].copy()

            for cell_info in notebook_data['cells']:
                await add_cell(cell_info['type'].lower(), initial_show_all_rows=cell_info.get('show_all_rows', False))

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
        for cell_data in self.cells[:]:
            cell_data['container'].delete()

        self.cells.clear()
        self.dataframes.clear()
        self.python_globals.clear()

    async def new_notebook(self):
        await self.clear_all_cells()
        self.current_filename = None
        self.mark_saved()
        await add_cell('sql')

    async def execute_python(self, code: str, show_all_rows_in_cell: bool) -> Tuple[bool, str, str, Optional[pd.DataFrame]]:
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()

        application_process_cwd = Path.cwd()

        output_type = 'text/plain'
        final_result_representation_parts = []
        figure_explicitly_handled = False
        last_displayed_or_returned_df: Optional[pd.DataFrame] = None

        try:
            user_working_dir = self.working_directory.resolve()
            if user_working_dir.is_dir():
                os.chdir(user_working_dir)
                logger.info(f"Changed CWD to: {user_working_dir} for Python execution.")
            else:
                logger.warning(f"User working directory '{user_working_dir}' is not a valid directory. "
                               f"Executing Python code in application CWD: '{application_process_cwd}'.")

            plt.close('all')

            exec_globals = {'pd': pd, 'np': np, 'asyncio': asyncio, 'plt': plt, **self.python_globals}

            def custom_display_func(obj):
                nonlocal final_result_representation_parts, output_type, figure_explicitly_handled, last_displayed_or_returned_df
                if isinstance(obj, pd.DataFrame):
                    if show_all_rows_in_cell:
                        max_rows_to_display = 200
                    else:
                        max_rows_to_display = 20

                    html_table = obj.to_html(classes='dataframe', border=0, max_rows=max_rows_to_display, escape=False)

                    message_suffix = ""
                    if not show_all_rows_in_cell and len(obj) > 20:
                        message_suffix = f"<p>*Showing first 20 of {len(obj)} rows. To see 200 rows, toggle 'Show all rows' in this cell's header.*</p>"
                    elif show_all_rows_in_cell and len(obj) > 200:
                        message_suffix = f"<p>*Showing first 200 of {len(obj)} rows due to display limit. Full DataFrame is available in memory.*</p>"
                    elif show_all_rows_in_cell and len(obj) > 20:
                        message_suffix = f"<p>*Showing all {len(obj)} rows.*</p>"

                    final_result_representation_parts.append(f"{html_table}{message_suffix}")
                    output_type = 'text/html'
                    last_displayed_or_returned_df = obj # Capture DataFrame

                elif isinstance(obj, matplotlib.figure.Figure):
                    buffer = io.BytesIO()
                    obj.savefig(buffer, format='png', bbox_inches='tight', pad_inches=0.1)

                    image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')

                    final_result_representation_parts.append(
                        f'<img src="data:image/png;base64,{image_base64}" style="max-width: 100%; height: auto; display: block; margin: 10px 0;"/>'
                    )
                    output_type = 'text/html'
                    plt.close(obj)
                    figure_explicitly_handled = True
                    last_displayed_or_returned_df = None # Clear if a figure is displayed

                elif obj is not None:
                    final_result_representation_parts.append(repr(obj))
                    if output_type != 'text/html':
                         output_type = 'text/plain'
                    last_displayed_or_returned_df = None # Clear for other types

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
                if not k.startswith('__') and k not in ['pd', 'np', 'display', 'asyncio', 'plt', 'matplotlib', 'io', 'base64']
            })

            std_out_content = captured_output.getvalue()

            if plt.get_fignums() and not figure_explicitly_handled:
                for fig_num in plt.get_fignums():
                    fig = plt.figure(fig_num)
                    custom_display_func(fig) # This will clear last_displayed_or_returned_df

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
                            # If the result is a DataFrame, custom_display_func will capture it
                            # in last_displayed_or_returned_df.
                            custom_display_func(evaluated_result)
                        except Exception:
                            pass

            final_result_representation = "\n".join(final_result_representation_parts)

            combined_output = ""
            if std_out_content:
                combined_output = f"<pre>{std_out_content.strip()}</pre>"
                if final_result_representation and output_type == 'text/html':
                    combined_output += f"\n{final_result_representation}"
                elif final_result_representation:
                    combined_output += f"\n<pre>{final_result_representation}</pre>"
                output_type = 'text/html' if (final_result_representation and output_type == 'text/html') or std_out_content else 'text/plain'
            elif final_result_representation:
                combined_output = final_result_representation

            if not combined_output.strip():
                combined_output = "Code executed successfully (no output)."
                output_type = 'text/plain'

            self.mark_modified()
            return True, combined_output, output_type, last_displayed_or_returned_df

        except Exception as e:
            error_message = f"Error: {str(e)}\n{traceback.format_exc()}"
            std_out_content = captured_output.getvalue()
            if std_out_content:
                error_message = f"{std_out_content}\n{error_message}"
            return False, error_message, 'text/plain', None
        finally:
            sys.stdout = old_stdout
            os.chdir(application_process_cwd)
            plt.close('all')
            logger.info(f"Restored CWD to: {application_process_cwd} after Python execution.")

notebook = NotebookApp()
ui.add_head_html(custom_css)
ui.add_head_html(DB_EXPLORER_CSS)
ui.add_body_html(DB_EXPLORER_JS)
ui.on('js_notify', lambda e: ui.notify(e.args[0], type=e.args[1]))


left_drawer_instance: Optional[ui.left_drawer] = None
right_drawer_instance: Optional[ui.right_drawer] = None
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
schema_container: Optional[ui.scroll_area] = None
schema_explorer_div: Optional[ui.html] = None
active_tab: Optional[ui.tabs] = None



async def pick_file_native(mode='save', file_types=None, initial_file: Optional[str] = None, initial_dir: Optional[str] = None) -> Optional[str]:
    if not TKINTER_AVAILABLE:
        logger.error("tkinter is not available for native file picker.")
        ui.notify("Native file picker is not available (tkinter module missing).", type='warning')
        return None

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

            _derived_defaultextension = ""
            if file_types and isinstance(file_types, list) and len(file_types) > 0:
                first_file_type_pattern = file_types[0][1]
                if isinstance(first_file_type_pattern, str) and first_file_type_pattern.startswith("*."):
                    if len(first_file_type_pattern) > 2 and first_file_type_pattern != "*.*":
                        _derived_defaultextension = first_file_type_pattern[1:]

            if not _derived_defaultextension and _initial_file:
                try:
                    ext = Path(_initial_file).suffix
                    if ext and ext != ".":
                        _derived_defaultextension = ext
                except Exception:
                    pass

            _title = "Save File As"
            if _initial_file and "_cell_" in _initial_file:
                _title = "Save Cell Code As"
            elif file_types and isinstance(file_types, list) and len(file_types) > 0:
                 first_file_type_label = file_types[0][0]
                 if "notebook" in first_file_type_label.lower():
                     _title = "Save Notebook As"

            filepath = filedialog.asksaveasfilename(
                initialdir=_initial_dir,
                initialfile=_initial_file,
                title=_title,
                filetypes=file_types,
                defaultextension=_derived_defaultextension
            )
        else:
            _title = "Open File"
            if file_types and isinstance(file_types, list) and len(file_types) > 0:
                 first_file_type_label = file_types[0][0]
                 if "notebook" in first_file_type_label.lower():
                     _title = "Open Notebook"

            filepath = filedialog.askopenfilename(
                initialdir=_initial_dir,
                title=_title,
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
    if not TKINTER_AVAILABLE:
        ui.notify("Native file picker is not available (tkinter module missing).", type='warning')
        return

    filepath = await pick_file_native(mode='save', file_types=[("Data Notebook", "*.dnb"), ("All Files", "*.*")])
    if filepath:
        success = notebook.save_notebook(filepath)
        if success:
            ui.notify(f"Notebook saved successfully!", type='positive')
            await update_working_directory_and_tree(str(notebook.working_directory))
        else:
            ui.notify("Failed to save notebook", type='negative')

async def handle_load_notebook():
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
    filepath = await pick_file_native(mode='open', file_types=[("Data Notebook", "*.dnb"), ("All Files", "*.*")])
    if filepath:
        success = await notebook.load_notebook(filepath)
        if success:
            ui.notify(f"Notebook loaded successfully!", type='positive')
        else:
            ui.notify("Failed to load notebook", type='negative')

async def handle_new_notebook():
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

async def save_cell_code(cell_data: Dict[str, Any]):
    code_content = cell_data['code'].value
    cell_type = cell_data['type'].value.lower()

    if not code_content.strip():
        ui.notify("Cell is empty. Nothing to save.", type='warning')
        return

    if cell_type == 'sql':
        file_extension = '.sql'
    elif cell_type == 'python':
        file_extension = '.py'
    else:
        ui.notify(f"Unsupported cell type for saving: {cell_type}", type='negative')
        return

    base_name_stem = "untitled"
    if notebook.current_filename:
        base_name_stem = Path(notebook.current_filename).stem

    filename_stem = f"{base_name_stem}_cell_{cell_data['id']}"

    target_dir = notebook.working_directory

    counter = 0
    current_filename_suggestion = f"{filename_stem}{file_extension}"
    actual_filepath = target_dir / current_filename_suggestion

    while actual_filepath.exists():
        counter += 1
        current_filename_suggestion = f"{filename_stem}_{counter}{file_extension}"
        actual_filepath = target_dir / current_filename_suggestion

    try:
        logger.info(f"Attempting to write cell code to: {actual_filepath}")
        actual_filepath.write_text(code_content, encoding='utf-8')
        ui.notify(f"Cell code saved as '{actual_filepath.name}' in working directory.", type='positive')
        await refresh_trees_ui()
    except Exception as e:
        logger.error(f"Failed to save cell code to {actual_filepath}: {e}", exc_info=True)
        ui.notify(f"Failed to save cell code: {e}", type='negative')

async def handle_download_csv(cell_data: Dict[str, Any]):
    """Handles downloading the DataFrame from a cell as CSV."""
    df_to_download = cell_data.get('df_to_download')
    cell_id = cell_data['id']
    cell_type_value = cell_data['type'].value # 'SQL' or 'Python'

    if df_to_download is None or not isinstance(df_to_download, pd.DataFrame):
        ui.notify("No DataFrame available to download for this cell.", type='warning')
        return

    base_filename_stem = "table_export"
    if cell_type_value == 'SQL':
        df_name_from_input = cell_data['df_name'].value.strip()
        base_filename_stem = df_name_from_input if df_name_from_input else f"sql_result_{cell_id}"
    elif cell_type_value == 'Python':
        py_df_var_name = None
        for name, var_instance in notebook.python_globals.items():
            if var_instance is df_to_download:
                py_df_var_name = name
                break
        if py_df_var_name:
            base_filename_stem = py_df_var_name
        else:
            base_filename_stem = f"python_output_{cell_id}"

    valid_chars = "-_.() abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    clean_filename_stem = ''.join(c for c in base_filename_stem if c in valid_chars)
    clean_filename_stem = clean_filename_stem.replace(' ', '_')
    if not clean_filename_stem:
        clean_filename_stem = f"exported_data_{cell_id}"

    filename = f"{clean_filename_stem}.csv"
    filepath = notebook.working_directory / filename

    counter = 1
    while filepath.exists():
        filename = f"{clean_filename_stem}_{counter}.csv"
        filepath = notebook.working_directory / filename
        counter += 1

    try:
        await asyncio.to_thread(df_to_download.to_csv, filepath, index=False)
        ui.notify(f"Table saved as '{filename}' in working directory.", type='positive')
        await refresh_trees_ui()
    except Exception as e:
        logger.error(f"Failed to save DataFrame as CSV to {filepath}: {e}", exc_info=True)
        ui.notify(f"Failed to save CSV: {e}", type='negative')

async def refresh_schema_explorer():
    """Refreshes the new JavaScript-based schema explorer."""
    global schema_container
    if not schema_container:
        return

    with schema_container:
        if not notebook.db_connection:
            schema_container.clear()
            with schema_container:
                ui.label("Not connected to database").classes('text-gray-500 p-4 text-center')
            return

        try:
            # Show a loading state
            schema_container.clear()
            with schema_container:
                ui.spinner(size='md').classes('mx-auto my-8')
                ui.label("Loading schema...").classes('text-center text-gray-500')

            # Fetch all data and cache it
            schema_data = await get_all_schema_data_optimized()
            if "error" in schema_data:
                raise Exception(schema_data["error"])

            notebook.db_schema_data = schema_data
            schema_names = sorted(list(schema_data.keys()))

            # Clear loading state and initialize JS component
            schema_container.clear()
            with schema_container:
                ui.html('<div id="db-explorer-container" style="margin: 0; padding: 0;"></div>')
                ui.run_javascript(f'''
                    new DbExplorer('db-explorer-container', {{ schemas: {json.dumps(schema_names)} }});
                ''')

        except Exception as e:
            logger.error(f"Error refreshing schema explorer: {e}", exc_info=True)
            schema_container.clear()
            with schema_container:
                ui.label(f"Error loading schema: {e}").classes('text-red-500 p-4')


async def refresh_trees_ui():
    """Refresh both file tree and schema tree if needed."""
    global file_tree, tree_container, notebook

    if active_tab.value == 'files':
        if not tree_container:
            return

        new_tree_nodes, new_state_snapshot = create_file_tree(path=notebook.working_directory, max_depth=3)

        if new_state_snapshot != notebook.last_tree_state:
            logger.info("File tree changed. Updating UI...")
            notebook.last_tree_state = new_state_snapshot
            tree_container.clear()

            with tree_container:
                if new_tree_nodes:
                    file_tree = ui.tree(new_tree_nodes, label_key='label', children_key='children', node_key='id').classes('w-full').style('margin-left: -10px; margin-top: -10px;')

                    def on_tree_double_click(event):
                        if event.args and event.args.get('node') and event.args['node'].get('is_file') and event.args['node'].get('path', '').endswith('.dnb'):
                            asyncio.create_task(load_notebook_from_path(event.args['node']['path']))

                    file_tree.on('dblclick', on_tree_double_click, ['node'])

                    expand_ids = [node['id'] for node in new_tree_nodes if not node.get('is_file', True) and 'children' in node]
                    if expand_ids:
                        file_tree.expand(expand_ids)

                    ui.timer(0.2, lambda: ui.run_javascript('colorizeDnbFiles()'), once=True)
                else:
                    ui.label("Directory is empty or inaccessible.").classes('q-pa-md text-caption text-[var(--text-secondary)]')
    elif active_tab.value == 'schema':
        # The schema explorer is now refreshed on its own schedule (on connect, on tab switch)
        pass

async def pick_directory_native() -> Optional[str]:
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
    if not TKINTER_AVAILABLE:
        ui.notify("Native directory picker is not available (tkinter module missing or failed to import).", type='warning')
        return

    selected_path_str = await pick_directory_native()
    if selected_path_str:
        logger.info(f"Native directory picker returned: {selected_path_str}")
        if working_dir_input:
            working_dir_input.set_value(selected_path_str)
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
        await refresh_trees_ui()
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

    await refresh_trees_ui()

async def load_notebook_from_path(filepath: str):
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

async def add_cell(cell_type='sql', initial_show_all_rows=False):
    cell_id = notebook.generate_cell_id()

    cell_data_dict = {
        'id': cell_id,
        'show_all_rows': initial_show_all_rows,
        'type': None, 'code': None, 'df_name': None, 'container': None,
        'execution_status': None, 'timer_label': None, 'spinner': None,
        'execution_result': None, 'result_icon': None, 'result_time': None,
        'is_collapsed': lambda: False, 'toggle_collapse': None,
        # NEW: For output and download button
        'output_container': None,
        'output_area_markdown': None,
        'download_button_row': None, # Keep this reference
        'df_to_download': None
    }

    with cell_container:
        cell_element = ui.column().classes('code-cell w-full cell-with-gutter')
        is_collapsed = False

        with cell_element:
            with ui.row().classes('code-cell-header w-full'):
                collapse_btn = ui.html('<button class="collapse-button"><span class="collapse-icon">🠉</span></button>')
                initial_select_value = 'Python' if cell_type == 'python' else cell_type.upper()
                cell_type_select = ui.select(options=['SQL', 'Python'], value=initial_select_value).classes('w-25 header-control-padding')
                df_name_input = ui.input(placeholder='Save to Dataframe', value='').style('width: 120px')
                df_name_input.visible = cell_type.upper() == 'SQL'

                show_all_rows_switch = ui.switch('Show all rows', value=cell_data_dict['show_all_rows']) \
                                        .classes('text-sm mr-2').props('dense color=primary')

                def on_show_all_rows_change(e):
                    cell_data_dict['show_all_rows'] = e.value
                    notebook.mark_modified()
                show_all_rows_switch.on_value_change(on_show_all_rows_change)

                cell_preview = ui.label('').classes('cell-preview')
                cell_preview.visible = False

                ui.space()
                save_cell_btn = ui.button(icon='save_alt', color='primary').classes('save-button')
                delete_btn = ui.button('✖', color='red').classes('delete-button').props('round')

            with ui.column().classes('code-cell-content w-full') as cell_content:
                cm_language = cell_type.lower()
                current_cm_theme = 'vscodeDark' if notebook.is_dark_mode else 'vscodeLight'
                code_editor = ui.codemirror(value='', language=cm_language, theme=current_cm_theme).classes('w-full code-editor')
                code_editor.props(f'data-cell-id="{cell_id}"')

                download_button_row_el = ui.row().classes('w-full justify-start pl-2 pt-1 pb-1 -mt-5 -mb-5') # Adjusted padding for placement
                with download_button_row_el:
                    download_csv_button = ui.button('Download Table as CSV', icon='download',
                                                    on_click=lambda: asyncio.create_task(handle_download_csv(cell_data_dict))) \
                                            .props('dense flat color=primary text-color=primary') \
                                            .style('font-size: 0.75rem; padding: 2px 6px;')
                download_button_row_el.visible = False
                # Output area structure
                output_container_el = ui.column().classes('output-container w-full')
                output_container_el.visible = False # Initially hidden
                with output_container_el:
                    output_area_markdown_el = ui.markdown('').classes('output-area-content w-full')
                    # Removed download_button_row_el from here

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

        async def run_cell():
            nonlocal is_collapsed
            if is_collapsed:
                toggle_collapse()
            code = code_editor.value
            cell_type_val = cell_type_select.value
            current_show_all_rows = cell_data_dict['show_all_rows']

            # Reset output state for the cell before running
            cell_data_dict['output_container'].visible = False
            cell_data_dict['download_button_row'].visible = False # Keep this line
            cell_data_dict['df_to_download'] = None
            cell_data_dict['output_area_markdown'].set_content('')


            logger.info(f"[{cell_id}] Run: {cell_type_val}, Code: {code[:50]!r}, Show All Rows: {current_show_all_rows}")
            if not code.strip():
                cell_data_dict['output_area_markdown'].set_content('No code to execute.')
                cell_data_dict['output_container'].visible = True
                return

            execution_status.visible = True
            execution_result.visible = False
            start_time = time.time()
            timer_active = True
            execution_success = False

            def update_timer():
                if timer_active:
                    elapsed_time = time.time() - start_time
                    timer_label.text = f'{elapsed_time:.1f}s'
            timer = ui.timer(0.1, update_timer)
            run_btn.disable()

            try:
                cell_data_dict['output_area_markdown'].set_content('Running...')
                cell_data_dict['output_container'].visible = True # Show "Running..."
                await asyncio.sleep(0.1)

                if cell_type_val == 'SQL':
                    df_name = df_name_input.value.strip()
                    result_df, message, saved_name = await notebook.execute_sql(code, df_name or None)
                    if result_df is not None:
                        execution_success = True
                        if current_show_all_rows: max_rows_to_display = 200
                        else: max_rows_to_display = 20

                        output_text = f"Shape: {result_df.shape}\n\n{result_df.to_html(classes='dataframe', border=0, max_rows=max_rows_to_display, escape=False)}"

                        if not current_show_all_rows and len(result_df) > 20:
                            output_text += f"\n\n*Showing first 20 of {len(result_df)} rows. To see 200 rows, toggle 'Show all rows' in this cell's header.*"
                        elif current_show_all_rows and len(result_df) > 200:
                            output_text += f"\n\n*Showing first 200 of {len(result_df)} rows due to display limit. Full DataFrame is available in memory.*"
                        elif current_show_all_rows and len(result_df) > 20:
                            output_text += f"\n\n*Showing all {len(result_df)} rows.*"

                        cell_data_dict['output_area_markdown'].set_content(output_text)
                        cell_data_dict['df_to_download'] = result_df # Store DF for download
                        cell_data_dict['download_button_row'].visible = True # Show download button
                        notebook.mark_modified()
                    else:
                        execution_success = False
                        cell_data_dict['output_area_markdown'].set_content(f"**SQL Error:** {message}")
                        ui.notify(f"Cell {cell_id}: SQL error.", type='negative')

                elif cell_type_val == 'Python':
                    success, py_output, py_output_type, last_df = await notebook.execute_python(code, current_show_all_rows)
                    execution_success = success
                    if success:
                        cell_data_dict['output_area_markdown'].set_content(py_output if py_output_type == 'text/html' else f"```\n{py_output}\n```")
                        if isinstance(last_df, pd.DataFrame):
                            cell_data_dict['df_to_download'] = last_df # Store DF for download
                            cell_data_dict['download_button_row'].visible = True # Show download button
                    else:
                        cell_data_dict['output_area_markdown'].set_content(f"**Python Error:**\n```\n{py_output}\n```")
                        ui.notify(f"Cell {cell_id}: Python error.", type='negative')

            except Exception as e:
                execution_success = False
                logger.error(f"[{cell_id}] run_cell error: {e}", exc_info=True)
                cell_data_dict['output_area_markdown'].set_content(f"**Unexpected Error:** {str(e)}\n{traceback.format_exc()}")
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
                # Ensure output container is visible if there's content OR download button is visible
                if cell_data_dict['output_area_markdown'].content or cell_data_dict['output_area_markdown']._props.get('innerHTML') or cell_data_dict['download_button_row'].visible:
                    cell_data_dict['output_container'].visible = True
                else:
                    cell_data_dict['output_container'].visible = False
            logger.info(f"[{cell_id}] End run_cell. Output container visible: {cell_data_dict['output_container'].visible}")

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

        cell_data_dict['toggle_collapse'] = toggle_collapse
        cell_data_dict['is_collapsed'] = lambda: is_collapsed

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

    cell_data_dict.update({
        'type': cell_type_select,
        'code': code_editor,
        'df_name': df_name_input,
        'container': cell_element,
        'execution_status': execution_status,
        'timer_label': timer_label,
        'spinner': spinner,
        'execution_result': execution_result,
        'result_icon': result_icon,
        'result_time': result_time,
        'output_container': output_container_el,
        'output_area_markdown': output_area_markdown_el,
        'download_button_row': download_button_row_el, # Ensure this is captured in the dict
    })
    notebook.cells.append(cell_data_dict)

    def delete_cell():
        notebook.cells.remove(cell_data_dict)
        cell_element.delete()
        notebook.mark_modified()

    run_btn.on_click(run_cell)
    delete_btn.on_click(delete_cell)
    save_cell_btn.on_click(functools.partial(save_cell_code, cell_data_dict))

    if hasattr(cell_container, '_add_cell_button'):
        cell_container._add_cell_button.move(target_index=-1)

async def add_cell_and_mark_modified(cell_type='sql'):
    await add_cell(cell_type)
    notebook.mark_modified()

async def setup_keyboard_shortcuts():
    ui.keyboard(on_key=handle_keyboard_shortcut)

def handle_keyboard_shortcut(event):
    if event.action.keydown:
        if event.modifiers.alt:
            if event.key.name == 's':
                asyncio.create_task(handle_save_notebook())
            elif event.key.name == 'o':
                asyncio.create_task(handle_load_notebook())
            elif event.key.name == 'n':
                asyncio.create_task(handle_new_notebook())
        elif event.modifiers.ctrl and event.key.name == 'b':
            if left_drawer_instance:
                left_drawer_instance.toggle()

def toggle_dark_mode():
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
    parts = Path(path).parts
    if len(parts) <= n:
        return str(path)
    return str(Path(*parts[-n:]))

main_container = ui.element('div').classes('w-full main-container')
ui.query('body').classes(add='dark-mode')

with main_container:
    with ui.column().classes('w-full h-full'):
        with ui.row().classes('toolbar w-full'):
            ui.button(on_click=lambda: left_drawer_instance.toggle(), icon='menu').props('flat round color=primary').classes('drawer-button-hover-effect').style('margin-left: -6px;')

            title_label = ui.label('Untitled').classes('text-2xl font-bold')
            notebook.title_label = title_label
            for _ in range(6):
                ui.row()

            with ui.row().classes('notebook-controls ml-15'):
                dark_mode_btn = ui.button(icon='light_mode', on_click=toggle_dark_mode).props('flat round')
                ui.button('New', on_click=handle_new_notebook).classes('save-load-button').tooltip('New Notebook')
                ui.button('Open', on_click=handle_load_notebook).classes('save-load-button').tooltip('Open Notebook')
                ui.button('Save', on_click=handle_save_notebook).classes('save-load-button').tooltip('Save Notebook (Alt+S)')

            with ui.row().classes('connection-status'):
                async def handle_reconnect():
                    if not notebook.last_successful_config:
                        ui.notify('No previous connection.', type='warning')
                        return
                    reconnect_btn.disable()
                    ui.notify('Reconnecting...', type='info')
                    reconnect_config = notebook.last_successful_config.copy()
                    if 'db_password' not in reconnect_config:
                        loaded_creds = notebook.load_credentials()
                        if loaded_creds.get('db_password'):
                            reconnect_config['db_password'] = loaded_creds['db_password']

                    success, message = await notebook.connect_to_database(reconnect_config)
                    status_indicator.content = f'<div class="status-indicator status_{"connected" if success else "disconnected"}"></div>'
                    status_label.text = "Connected" if success else "Disconnected"
                    ui.notify(f'Reconnection {"successful" if success else "failed"}: {message}', type='positive' if success else 'negative')
                    reconnect_btn.enable()

                    if success and active_tab.value == 'schema':
                        await refresh_schema_explorer()

                # reconnect_btn = ui.button(icon='refresh', on_click=handle_reconnect).props('flat round dense').tooltip('Reconnect')
                # reconnect_btn.visible = False
                connect_btn = ui.button('Connect', on_click=lambda: connection_dialog.open()).classes('connect-button').tooltip('Configure DB Connection')
                status_indicator = ui.html('<div class="status-indicator status-disconnected"></div>')
                status_label = ui.label('Disconnected')

            ui.button(icon='auto_awesome', on_click=lambda: right_drawer_instance.toggle()) \
                .props('flat round color=orange').classes('drawer-button-hover-effect') \
                .tooltip('Toggle AI Assistant')

        cell_container = ui.column().classes('w-full cell-container')
        with cell_container:
            cell_container._add_cell_button = ui.button('+ Add Cell', on_click=lambda: asyncio.create_task(add_cell_and_mark_modified('sql'))).classes('add-cell-button').props('id=add-cell-button')

with ui.left_drawer(value=False, elevated=False, top_corner=False, bordered=True) \
        .props('width=240 behavior=desktop') \
        .classes('bg-[var(--bg-primary)]') \
        .style('padding: 0; margin: 0') as drawer:
    left_drawer_instance = drawer

    with ui.column().classes('w-full h-full no-wrap'):
        # Tabs for Files and Schema
        with ui.tabs().classes('w-full bg-primary').props('dense align=justify').style('height: 45px;') as tabs:
            active_tab = tabs
            files_tab = ui.tab('files', label='Files', icon='folder').style('height: 44px; min-height: 44px;').classes('small-tab-label')
            schema_tab = ui.tab('schema', label='Schema', icon='lan').style('height: 44px; min-height: 44px;').classes('small-tab-label')

        with ui.tab_panels(tabs, value='files').classes('w-full flex-grow p-0 m-0'):
            # Files Tab Panel
            with ui.tab_panel('files').classes('p-0'):
                with ui.column().classes('w-full h-full no-wrap'):
                    # Working directory controls
                    with ui.row().classes('items-center w-full px-2 py-1'):
                        browse_wd_button = ui.button(icon='folder_open', on_click=handle_browse_working_directory) \
                            .classes('browse-wd-button-hover-effect') \
                            .style('width: 24px; height: 24px; font-size: 12px; flex-shrink: 0; margin-right: -9px;').props('round')

                        if not TKINTER_AVAILABLE:
                            browse_wd_button.disable()
                            browse_wd_button.tooltip("Native directory picker unavailable (tkinter missing)")

                        display_path = get_last_n_path_parts(str(notebook.working_directory), 2)
                        working_dir_display = ui.label(display_path).style('font-size: 14px;')

                    # File tree
                    with ui.scroll_area().classes('flex-grow min-h-0 w-full') as tc_instance:
                        tree_container = tc_instance
                        initial_tree_nodes, initial_state_snapshot = create_file_tree(path=notebook.working_directory, max_depth=3)
                        notebook.last_tree_state = initial_state_snapshot

                        file_tree = ui.tree(initial_tree_nodes, label_key='label', children_key='children', node_key='id').classes('w-full').style('margin-left: -10px; margin-top: -10px;')

                        def on_tree_double_click(event):
                             if event.args and event.args.get('node') and event.args['node'].get('is_file') and event.args['node'].get('path', '').endswith('.dnb'):
                                asyncio.create_task(load_notebook_from_path(event.args['node']['path']))

                        file_tree.on('dblclick', on_tree_double_click, ['node'])

                        if initial_tree_nodes:
                            expand_ids = [node['id'] for node in initial_tree_nodes if not node.get('is_file', True) and 'children' in node]
                            if expand_ids:
                                file_tree.expand(expand_ids)
                        else:
                            ui.label("Directory is empty or inaccessible.").classes('q-pa-md text-caption text-[var(--text-secondary)]')

                        ui.timer(0.2, lambda: ui.run_javascript('colorizeDnbFiles()'), once=True)

            # Schema Tab Panel
            with ui.tab_panel('schema').classes('-mt-4'):
                with ui.row().classes('ml-3 gap-2'):  # Added gap-2 for spacing between buttons
                    # Move the reconnect button here
                    async def handle_reconnect():
                        if not notebook.last_successful_config:
                            ui.notify('No previous connection.', type='warning')
                            return
                        reconnect_btn.disable()
                        reconnect_config = notebook.last_successful_config.copy()
                        if 'db_password' not in reconnect_config:
                            loaded_creds = notebook.load_credentials()
                            if loaded_creds.get('db_password'):
                                reconnect_config['db_password'] = loaded_creds['db_password']

                        success, message = await notebook.connect_to_database(reconnect_config)
                        # Update status in toolbar
                        status_indicator.content = f'<div class="status-indicator status_{"connected" if success else "disconnected"}"></div>'
                        status_label.text = "Connected" if success else "Disconnected"
                        ui.notify(f'Reconnection {"successful" if success else "failed"}: {message}', type='positive' if success else 'negative')
                        reconnect_btn.enable()

                        if success:
                            await refresh_schema_explorer()

                    def collapse_all_tables():
                        # JavaScript to collapse all table nodes in the schema explorer
                        ui.run_javascript('''
                            document.querySelectorAll('#db-explorer-container .tree-node--table[aria-expanded="true"]').forEach(node => {
                                node.setAttribute('aria-expanded', 'false');
                            });
                        ''')

                    reconnect_btn = ui.button(icon='refresh', on_click=handle_reconnect).props('flat round=sm')
                    reconnect_btn.visible = False
                    
                    collapse_btn = ui.button('Collapse All', icon='unfold_less', on_click=collapse_all_tables).props('flat').style('font-size: 0.85rem;')
                    collapse_btn.visible = False  # Show/hide along with reconnect button
                
                # The scroll area will contain the JS-rendered tree
                with ui.scroll_area().classes('flex-grow min-h-0 w-full p-0 m-0 -mt-7') as sc_instance:
                    schema_container = sc_instance
                    ui.label("Not connected to database").classes('text-gray-500 p-4 text-center')

async def on_tab_change(e):
    """Handle tab switching efficiently"""
    if e.value == 'schema':
        await refresh_schema_explorer()
    elif e.value == 'files':
        await refresh_trees_ui()

active_tab.on_value_change(on_tab_change)

with ui.right_drawer(value=False, elevated=False, top_corner=False, bordered=True) \
        .props('width=450') \
        .classes('bg-[var(--bg-primary)]') as r_drawer:
    right_drawer_instance = r_drawer
    with ui.column().classes('w-full h-full no-wrap pa-0'):
        ui.html(
            f'<iframe src="https://aistudio.google.com/prompts/new_chat" '
            f'style="width: 100%; height: 100%; border: none; display: block;"></iframe>'
        ).classes('w-full h-full')


with ui.dialog() as connection_dialog:
    with ui.card().classes('w-96'):
        saved_creds = notebook.load_credentials()
        ui.label('Database Configuration (Required)').classes('text-lg font-semibold mb-2')
        db_host = ui.input('Database Host', value=saved_creds.get('db_host', ''), placeholder='')
        db_port = ui.input('Database Port', value=saved_creds.get('db_port', '5439'), placeholder='')
        db_name = ui.input('Database Name', value=saved_creds.get('db_name', ''), placeholder='')
        db_user = ui.input('Database User', value=saved_creds.get('db_user', ''), placeholder='')
        db_password = ui.input('Database Password', value=saved_creds.get('db_password', ''), placeholder='').props('type=password')
        ui.separator().classes('my-4')
        with ui.expansion('SSH Configuration (Optional)', icon='vpn_key').classes('w-full'):
            ui.label('Configure SSH tunnel for secure database connections').classes('text-sm text-white-500 mb-2')
            ssh_host = ui.input('SSH Host', value=saved_creds.get('ssh_host', ''), placeholder='')
            ssh_port = ui.input('SSH Port', value=saved_creds.get('ssh_port', '22'), placeholder='')
            ssh_username = ui.input('SSH Username', value=saved_creds.get('ssh_username', ''), placeholder='')
            ssh_key_path = ui.input('SSH Private Key Path', value=saved_creds.get('ssh_private_key', ''), placeholder='')

        save_creds_checkbox = ui.checkbox('Save connection details (including password)', value=bool(saved_creds.get('db_password')))

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
                    collapse_btn.visible = True

                    await refresh_schema_explorer()

                    if save_creds_checkbox.value:
                        notebook.save_credentials(config)
                    else:
                        if notebook.credentials_file.exists():
                            notebook.credentials_file.unlink()
                        ui.notify("Credentials not saved.", type='info')

                    connection_dialog.close()
                else:
                    ui.notify(f'Connection failed: {message}', type='negative')
            ui.button('Connect', on_click=connect_action).classes('bg-blue-500')

async def initialize_app():
    await add_cell('sql')
    if notebook.cells and hasattr(notebook.cells[0]['code'], 'theme'):
        notebook.cells[0]['code'].theme = 'vscodeDark'

    await setup_keyboard_shortcuts()

    ui.timer(0.5, refresh_trees_ui, once=True)

ui.timer(0.1, initialize_app, once=True)

reload_dir = str(Path(__file__).resolve().parent)
app_source_dir = str(Path(__file__).resolve().parent)

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(
        title='Notebook',
        port=8080,
        native=False,
        reload=False,
        show=True,
        favicon='🔶',
        uvicorn_reload_dirs=None,
        storage_secret="a_nice_secret_key_for_storage"
    )