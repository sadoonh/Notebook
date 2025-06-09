#!/usr/bin/env python3
"""
Database Explorer - PostgreSQL with NiceGUI connection form and a production-ready JavaScript tree.
Install: pip install nicegui psycopg2-binary
Run: python main.py
"""

from nicegui import ui, app, run
import psycopg2
import json

# Global connection
current_connection = None

# --- Database Logic ---

def connect_to_database(host, port, database, username, password):
    """Connect to PostgreSQL. This is a blocking I/O call."""
    return psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=username,
        password=password
    )

def get_schemas(conn):
    """Get all schemas from PostgreSQL."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name
    """)
    schemas = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return schemas

def get_tables(conn, schema):
    """Get all tables in a schema."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """, (schema,))
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables

def get_columns(conn, schema, table):
    """Get all columns in a table with primary key information."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            c.column_name, 
            c.data_type, 
            c.is_nullable,
            CASE 
                WHEN pk.column_name IS NOT NULL THEN 'YES'
                ELSE 'NO'
            END as is_primary_key
        FROM information_schema.columns c
        LEFT JOIN (
            SELECT 
                kcu.column_name,
                kcu.table_name,
                kcu.table_schema
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name 
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
        ) pk ON c.column_name = pk.column_name 
             AND c.table_name = pk.table_name 
             AND c.table_schema = pk.table_schema
        WHERE c.table_schema = %s AND c.table_name = %s
        ORDER BY c.ordinal_position
    """, (schema, table))
    columns = [(row[0], row[1], row[2], row[3]) for row in cursor.fetchall()]
    cursor.close()
    return columns

def check_connection_health(conn):
    """Check if the connection is still alive."""
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        cursor.close()
        return True
    except Exception:
        return False

# --- API Endpoints for JavaScript Client ---

@app.get('/api/tables/{schema}')
async def get_tables_api(schema: str):
    if not current_connection:
        return {'success': False, 'error': 'Not connected to database.'}
    
    # Add connection health check
    if not check_connection_health(current_connection):
        return {'success': False, 'error': 'Database connection lost.'}
    
    try:
        tables = get_tables(current_connection, schema)
        return {'success': True, 'tables': tables}
    except Exception as e:
        return {'success': False, 'error': f'Database error: {str(e)}'}

@app.get('/api/columns/{schema}/{table}')
async def get_columns_api(schema: str, table: str):
    if not current_connection:
        return {'success': False, 'error': 'Not connected to database.'}
    
    # Add connection health check
    if not check_connection_health(current_connection):
        return {'success': False, 'error': 'Database connection lost.'}
    
    try:
        columns = get_columns(current_connection, schema, table)
        return {'success': True, 'columns': columns}
    except Exception as e:
        return {'success': False, 'error': f'Database error: {str(e)}'}

# --- NiceGUI User Interface ---

@ui.page('/')
def main_page():
    global current_connection

    ui.label('🗄️ Database Explorer').classes('text-3xl font-bold text-center mb-6')

    # Connection form
    with ui.card().classes('w-full max-w-md mx-auto p-6') as connection_form:
        ui.label('Connect to PostgreSQL').classes('text-xl font-semibold mb-4')
        host_input = ui.input('Host', value='localhost').classes('w-full mb-2')
        port_input = ui.input('Port', value='5432').classes('w-full mb-2')
        database_input = ui.input('Database', value='postgres').classes('w-full mb-2')
        username_input = ui.input('Username', value='postgres').classes('w-full mb-2')
        password_input = ui.input('Password', password=True).classes('w-full mb-4')
        connect_button = ui.button('Connect').classes('w-full bg-blue-500 text-white')
        status_label = ui.label('Ready to connect').classes('text-sm mt-2 text-gray-500')

    # Tree view container (initially hidden)
    with ui.card().classes('w-full max-w-3xl mx-auto p-4') as tree_section:
        tree_section.set_visibility(False)
        with ui.row().classes('w-full justify-between items-center mb-2'):
            connection_status = ui.label().classes('text-lg font-semibold')
            disconnect_button = ui.button('Disconnect').classes('bg-red-500 text-white')
        ui.separator().classes('mb-4')
        ui.html('<div id="db-explorer-container"></div>')

    async def connect_database():
        global current_connection
        try:
            status_label.text = "Connecting..."
            status_label.classes(remove='text-red-500', add='text-blue-500')
            connect_button.set_text('Connecting...')
            connect_button.disable()

            # CORRECT WAY: Use run.io_bound for blocking I/O calls to prevent freezing the UI.
            conn = await run.io_bound(connect_to_database,
                host_input.value, int(port_input.value), database_input.value,
                username_input.value, password_input.value
            )
            schemas = await run.io_bound(get_schemas, conn)
            current_connection = conn

            # Update UI state
            connection_form.set_visibility(False)
            tree_section.set_visibility(True)
            connection_status.text = f"Connected to {database_input.value} on {host_input.value}"

            # Initialize the JavaScript component with data from Python.
            # json.dumps safely formats the list of schemas into a JS array.
            ui.run_javascript(f'''
                const container = document.getElementById('db-explorer-container');
                if (container) {{ container.innerHTML = ''; }}
                new DbExplorer('db-explorer-container', {{ schemas: {json.dumps(schemas)} }});
            ''')

            status_label.text = f"Connected successfully! Found {len(schemas)} schemas."
            status_label.classes(remove='text-blue-500', add='text-green-500')

        except Exception as e:
            status_label.text = f"Connection failed: {e}"
            status_label.classes(remove='text-blue-500 text-green-500', add='text-red-500')
        finally:
            connect_button.set_text('Connect')
            connect_button.enable()

    def disconnect_database():
        global current_connection
        if current_connection:
            current_connection.close()
            current_connection = None

        connection_form.set_visibility(True)
        tree_section.set_visibility(False)
        status_label.text = "Disconnected"
        status_label.classes(remove='text-red-500 text-green-500 text-blue-500', add='text-gray-500')
        ui.run_javascript("document.getElementById('db-explorer-container').innerHTML = '';")

    connect_button.on('click', connect_database)
    disconnect_button.on('click', disconnect_database)

    # --- Inject Production-Ready CSS into the <head> ---
    ui.add_head_html('''
        <style>
            :root {
                --font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                --font-size-base: 14px;
                --color-text-primary: #212529;
                --color-text-secondary: #6c757d;
                --color-text-muted: #868e96;
                --color-border: #dee2e6;
                --color-bg-hover: #f1f3f5;
                --color-schema: #0d6efd;
                --color-table: #198754;
                --color-column: #343a40;
                --color-pk-bg: #d1ecf1;
                --color-pk-text: #0c5460;
                --spacing-unit: 8px;
            }
            #db-explorer-container {
                font-family: var(--font-family);
                font-size: var(--font-size-base);
                color: var(--color-text-primary);
            }
            .tree-node { position: relative; }
            .tree-node-content {
                margin-left: calc(var(--spacing-unit) * 3);
                padding-left: calc(var(--spacing-unit) * 2);
                border-left: 1px solid var(--color-border);
            }
            .tree-node-header {
                display: flex;
                align-items: center;
                padding: calc(var(--spacing-unit) / 2) 0;
                width: 100%;
                background: none;
                border: none;
                text-align: left;
                cursor: pointer;
                border-radius: 4px;
            }
            .tree-node-header:hover { background-color: var(--color-bg-hover); }
            .tree-node-header .icon-toggle {
                flex-shrink: 0;
                width: 16px; height: 16px;
                margin-right: var(--spacing-unit);
                color: var(--color-text-secondary);
                transition: transform 0.2s ease-in-out;
            }
            .tree-node[aria-expanded="true"] > .tree-node-header .icon-toggle { transform: rotate(90deg); }
            .tree-node-label { font-weight: 500; cursor: pointer; }
            .tree-node-label:hover { text-decoration: underline; }
            .tree-node--schema > .tree-node-header .tree-node-label { color: var(--color-schema); }
            .tree-node--table > .tree-node-header .tree-node-label { color: var(--color-table); }
            .tree-node > .tree-node-content { display: none; }
            .tree-node[aria-expanded="true"] > .tree-node-content { display: block; }
            .column-item {
                display: flex;
                align-items: center;
                padding: calc(var(--spacing-unit) / 4) 0;
                font-size: 13px;
                cursor: pointer;
                border-radius: 4px;
                transition: background-color 0.2s ease;
            }
            .column-item:hover { background-color: var(--color-bg-hover); }
            .column-name { color: var(--color-column); min-width: 150px; }
            .column-type { color: var(--color-text-muted); font-style: italic; margin-right: var(--spacing-unit); }
            .pk-badge {
                background-color: var(--color-pk-bg);
                color: var(--color-pk-text);
                padding: 2px 5px; border-radius: 3px;
                font-size: 10px; font-weight: 600;
            }
            .spinner {
                display: none;
                width: 16px; height: 16px;
                border: 2px solid currentColor;
                border-top-color: transparent;
                border-radius: 50%;
                animation: spin 0.8s linear infinite;
                margin-left: auto;
                margin-right: var(--spacing-unit);
                color: var(--color-text-secondary);
            }
            .tree-node[aria-busy="true"] > .tree-node-header .spinner { display: inline-block; }
            .error-message { color: #dc3545; font-style: italic; padding: var(--spacing-unit) 0; font-size: 13px; }
            @keyframes spin { to { transform: rotate(360deg); } }
        </style>
    ''')

    # --- Inject Production-Ready JavaScript into the <body> ---
    ui.add_body_html('''
        <script>
        class DbExplorer {
            constructor(elementId, initialData) {
                this.container = document.getElementById(elementId);
                if (!this.container) {
                    throw new Error(`Element with ID "${elementId}" not found.`);
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
                
                // Handle column name clicks
                if (columnItem) {
                    event.preventDefault();
                    event.stopPropagation();
                    const columnName = columnItem.querySelector('.column-name').textContent;
                    await this.copyToClipboard(columnName, 'Column name');
                    return;
                }
                
                if (!header) return;

                // Check if clicked on the label specifically
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
                
                // Debug logging
                console.log('Node clicked:', {
                    type: nodeType,
                    expanded: isExpanded,
                    busy: isBusy,
                    schema: node.dataset.schema,
                    table: node.dataset.table
                });
                
                // Don't do anything if already busy
                if (isBusy) {
                    console.log('Node is busy, ignoring click');
                    return;
                }

                if (isExpanded) {
                    this.collapseNode(node);
                } else {
                    try {
                        await this.expandNode(node, nodeType);
                    } catch (error) {
                        console.error('Error in handleTreeClick:', error);
                    }
                }
            }

            async expandNode(node, nodeType) {
                // Check if already expanded with content
                if (node.querySelector('.tree-node-content')) {
                    node.setAttribute('aria-expanded', 'true');
                    return;
                }
                
                // Set loading state
                node.setAttribute('aria-busy', 'true');
                node.setAttribute('aria-expanded', 'true');

                try {
                    let content;
                    if (nodeType === 'schema') {
                        content = await this.loadTables(node.dataset.schema);
                    } else if (nodeType === 'table') {
                        content = await this.loadColumns(node.dataset.schema, node.dataset.table);
                    } else {
                        throw new Error(`Unknown node type: ${nodeType}`);
                    }
                    
                    // Only append if we got valid content
                    if (content) {
                        node.appendChild(content);
                    } else {
                        throw new Error('No content returned');
                    }
                    
                } catch (err) {
                    console.error('Error expanding node:', err);
                    
                    // Reset to collapsed state on error
                    node.setAttribute('aria-expanded', 'false');
                    
                    // Show error message
                    const errorDiv = document.createElement('div');
                    errorDiv.className = 'tree-node-content';
                    errorDiv.innerHTML = `<div class="error-message">Failed to load: ${err.message}</div>`;
                    node.appendChild(errorDiv);
                    
                    // Set back to expanded to show error
                    node.setAttribute('aria-expanded', 'true');
                    
                } finally {
                    // Always clear loading state
                    node.setAttribute('aria-busy', 'false');
                }
            }

            collapseNode(node) {
                node.setAttribute('aria-expanded', 'false');
            }

            async loadTables(schema) {
                try {
                    const response = await fetch(`/api/tables/${encodeURIComponent(schema)}`);
                    
                    if (!response.ok) {
                        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                    }
                    
                    const result = await response.json();
                    const contentDiv = document.createElement('div');
                    contentDiv.className = 'tree-node-content';

                    if (!result.success) {
                        throw new Error(result.error || 'Unknown API error');
                    }

                    if (result.tables && result.tables.length > 0) {
                        result.tables.forEach(table => {
                            const tableNode = this.createNode('table', table, { schema, table });
                            contentDiv.appendChild(tableNode);
                        });
                    } else {
                        contentDiv.innerHTML = '<i style="padding: 4px 0; display: block;">No tables found in this schema.</i>';
                    }
                    
                    return contentDiv;
                    
                } catch (error) {
                    console.error('Error loading tables:', error);
                    throw error; // Re-throw to be handled by expandNode
                }
            }

            async loadColumns(schema, table) {
                try {
                    const response = await fetch(`/api/columns/${encodeURIComponent(schema)}/${encodeURIComponent(table)}`);
                    
                    if (!response.ok) {
                        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                    }
                    
                    const result = await response.json();
                    const contentDiv = document.createElement('div');
                    contentDiv.className = 'tree-node-content';

                    if (!result.success) {
                        throw new Error(result.error || 'Unknown API error');
                    }

                    if (result.columns && result.columns.length > 0) {
                        result.columns.forEach(([name, type, nullable, isPrimaryKey]) => {
                            const normalizedType = this.normalizeDataType(type);
                            const pkBadge = isPrimaryKey === 'YES' ? `<span class="pk-badge">PK</span>` : '';
                            const columnEl = document.createElement('div');
                            columnEl.className = 'column-item';
                            columnEl.style.cursor = 'pointer';
                            columnEl.title = `Click to copy column name: ${name}`;
                            columnEl.innerHTML = `
                                <span class="column-name">${this.escapeHtml(name)}</span>
                                <span class="column-type">${this.escapeHtml(normalizedType)}</span>
                                ${pkBadge}
                            `;
                            contentDiv.appendChild(columnEl);
                        });
                    } else {
                        contentDiv.innerHTML = '<i style="padding: 4px 0; display: block;">No columns found in this table.</i>';
                    }
                    
                    return contentDiv;
                    
                } catch (error) {
                    console.error('Error loading columns:', error);
                    throw error;
                }
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
                        <span class="tree-node-label" title="Click to copy ${type} name: ${label}">${this.escapeHtml(label)}</span>
                        <div class="spinner"></div>
                    </button>
                `;
                return node;
            }
            
            // Add this helper method for security
            escapeHtml(text) {
                const div = document.createElement('div');
                div.textContent = text;
                return div.innerHTML;
            }
            
            // Normalize PostgreSQL data types for display
            normalizeDataType(pgType) {
                const typeMap = {
                    'character varying': 'varchar',
                    'timestamp without time zone': 'timestamp',
                    'timestamp with time zone': 'timestamptz'
                };
                
                return typeMap[pgType] || pgType;
            }
            
            // Copy to clipboard functionality
            async copyToClipboard(text, itemType) {
                try {
                    await navigator.clipboard.writeText(text);
                    this.showCopyNotification(`${itemType} "${text}" copied to clipboard!`);
                } catch (err) {
                    // Fallback for older browsers
                    const textArea = document.createElement('textarea');
                    textArea.value = text;
                    textArea.style.position = 'fixed';
                    textArea.style.left = '-999999px';
                    textArea.style.top = '-999999px';
                    document.body.appendChild(textArea);
                    textArea.focus();
                    textArea.select();
                    
                    try {
                        document.execCommand('copy');
                        this.showCopyNotification(`${itemType} "${text}" copied to clipboard!`);
                    } catch (fallbackErr) {
                        console.error('Failed to copy text:', fallbackErr);
                        this.showCopyNotification(`Failed to copy ${itemType}`, 'error');
                    }
                    
                    document.body.removeChild(textArea);
                }
            }
            
            // Show copy notification
            showCopyNotification(message, type = 'success') {
                // Remove any existing notification
                const existingNotification = document.querySelector('.copy-notification');
                if (existingNotification) {
                    existingNotification.remove();
                }
                
                const notification = document.createElement('div');
                notification.className = `copy-notification copy-notification--${type}`;
                notification.textContent = message;
                
                // Position at top right of the container
                notification.style.cssText = `
                    position: fixed;
                    top: 20px;
                    right: 20px;
                    background: ${type === 'error' ? '#dc3545' : '#28a745'};
                    color: white;
                    padding: 12px 16px;
                    border-radius: 4px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.15);
                    z-index: 1000;
                    font-size: 14px;
                    max-width: 300px;
                    word-wrap: break-word;
                    opacity: 0;
                    transition: opacity 0.3s ease;
                `;
                
                document.body.appendChild(notification);
                
                // Fade in
                setTimeout(() => {
                    notification.style.opacity = '1';
                }, 10);
                
                // Auto remove after 3 seconds
                setTimeout(() => {
                    notification.style.opacity = '0';
                    setTimeout(() => {
                        if (notification.parentNode) {
                            notification.parentNode.removeChild(notification);
                        }
                    }, 300);
                }, 3000);
            }
            
            renderError(message) {
                this.container.innerHTML = `<div class="error-message" style="padding: 16px;">${this.escapeHtml(message)}</div>`;
            }
        }
        </script>
    ''')

if __name__ in {"__main__", "__mp_main__"}:
    print("🗄️ Database Explorer")
    print("📦 Install: pip install nicegui psycopg2-binary")
    print("🚀 Starting server...")
    
    ui.run(
        title="Database Explorer",
        favicon="🗄️",
        reload=False
    )