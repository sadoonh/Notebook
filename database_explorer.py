#!/usr/bin/env python3
"""
Database Explorer - PostgreSQL with NiceGUI connection form and JavaScript tree
Install: pip install nicegui psycopg2-binary
Run: python main.py
"""

from nicegui import ui, app
import psycopg2

# Global connection
current_connection = None

def connect_to_database(host, port, database, username, password):
    """Connect to PostgreSQL"""
    return psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=username,
        password=password
    )

def get_schemas(conn):
    """Get all schemas from PostgreSQL"""
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
    """Get all tables in a schema"""
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
    """Get all columns in a table"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (schema, table))
    columns = [(row[0], row[1], row[2]) for row in cursor.fetchall()]
    cursor.close()
    return columns

# API endpoints for tree operations
@app.get('/api/tables/{schema}')
async def get_tables_api(schema: str):
    try:
        tables = get_tables(current_connection, schema)
        return {'success': True, 'tables': tables}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.get('/api/columns/{schema}/{table}')
async def get_columns_api(schema: str, table: str):
    try:
        columns = get_columns(current_connection, schema, table)
        return {'success': True, 'columns': columns}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@ui.page('/')
def main_page():
    global current_connection
    
    ui.label('🗄️ Database Explorer').classes('text-3xl font-bold text-center mb-6')
    
    # Connection form using NiceGUI
    with ui.card().classes('w-full max-w-md mx-auto p-6') as connection_form:
        ui.label('Connect to PostgreSQL').classes('text-xl font-semibold mb-4')
        
        host_input = ui.input('Host', value='localhost').classes('w-full mb-2')
        port_input = ui.input('Port', value='5432').classes('w-full mb-2')
        database_input = ui.input('Database').classes('w-full mb-2')
        username_input = ui.input('Username').classes('w-full mb-2')
        password_input = ui.input('Password', password=True).classes('w-full mb-4')
        
        connect_button = ui.button('Connect').classes('w-full bg-blue-500 text-white')
        status_label = ui.label('Ready to connect').classes('text-sm mt-2 text-gray-500')
    
    # Tree view container (initially hidden)
    with ui.card().classes('w-full max-w-4xl mx-auto p-6') as tree_section:
        tree_section.set_visibility(False)
        
        with ui.row().classes('w-full justify-between items-center mb-4'):
            connection_status = ui.label('Connected').classes('text-lg font-semibold')
            disconnect_button = ui.button('Disconnect').classes('bg-red-500 text-white')
        
        ui.separator().classes('mb-4')
        ui.label('Database Structure').classes('text-xl font-semibold mb-2')
        
        # JavaScript tree container
        ui.html('<div id="tree-container" style="font-family: monospace; font-size: 14px;"></div>')
    
    async def connect_database():
        global current_connection
        
        try:
            status_label.text = "Connecting..."
            status_label.classes('text-blue-500')
            connect_button.set_text('Connecting...')
            
            # Test connection
            conn = connect_to_database(
                host_input.value,
                int(port_input.value),
                database_input.value,
                username_input.value,
                password_input.value
            )
            
            schemas = get_schemas(conn)
            current_connection = conn
            
            # Update UI
            connection_form.set_visibility(False)
            tree_section.set_visibility(True)
            connection_status.text = f"Connected to {database_input.value}"
            
            # Initialize tree with JavaScript
            ui.run_javascript(f'''
                window.dbExplorer = {{
                    schemas: {schemas},
                    expandedSchemas: new Set(),
                    expandedTables: new Set(),
                    schemaData: {{}}
                }};
                renderTree();
            ''')
            
            status_label.text = f"Connected successfully! Found {len(schemas)} schemas."
            status_label.classes('text-green-500')
            
        except Exception as e:
            status_label.text = f"Connection failed: {str(e)}"
            status_label.classes('text-red-500')
        finally:
            connect_button.set_text('Connect')
    
    def disconnect_database():
        global current_connection
        
        if current_connection:
            current_connection.close()
            current_connection = None
        
        connection_form.set_visibility(True)
        tree_section.set_visibility(False)
        status_label.text = "Disconnected"
        status_label.classes('text-gray-500')
    
    # Note: API endpoints are defined outside this function
    
    connect_button.on('click', connect_database)
    disconnect_button.on('click', disconnect_database)
    
    # Add JavaScript for tree functionality
    ui.add_body_html('''
        <script>
            window.dbExplorer = {
                schemas: [],
                expandedSchemas: new Set(),
                expandedTables: new Set(),
                schemaData: {}
            };
            
            async function toggleSchema(schema) {
                if (window.dbExplorer.expandedSchemas.has(schema)) {
                    window.dbExplorer.expandedSchemas.delete(schema);
                } else {
                    window.dbExplorer.expandedSchemas.add(schema);
                    if (!window.dbExplorer.schemaData[schema]) {
                        await loadTables(schema);
                    }
                }
                renderTree();
            }
            
            async function loadTables(schema) {
                try {
                    const response = await fetch(`/api/tables/${schema}`);
                    const result = await response.json();
                    if (result.success) {
                        window.dbExplorer.schemaData[schema] = { tables: result.tables, columns: {} };
                    }
                } catch (err) {
                    console.error('Failed to load tables:', err);
                }
            }
            
            async function toggleTable(schema, table) {
                const tableKey = `${schema}.${table}`;
                
                if (window.dbExplorer.expandedTables.has(tableKey)) {
                    window.dbExplorer.expandedTables.delete(tableKey);
                } else {
                    window.dbExplorer.expandedTables.add(tableKey);
                    if (!window.dbExplorer.schemaData[schema].columns[table]) {
                        await loadColumns(schema, table);
                    }
                }
                renderTree();
            }
            
            async function loadColumns(schema, table) {
                try {
                    const response = await fetch(`/api/columns/${schema}/${table}`);
                    const result = await response.json();
                    if (result.success) {
                        window.dbExplorer.schemaData[schema].columns[table] = result.columns;
                        renderTree();
                    }
                } catch (err) {
                    console.error('Failed to load columns:', err);
                }
            }
            
            function renderTree() {
                const container = document.getElementById('tree-container');
                if (!container) return;
                
                let html = '';
                
                window.dbExplorer.schemas.forEach(schema => {
                    const isExpanded = window.dbExplorer.expandedSchemas.has(schema);
                    const expandIcon = isExpanded ? '▼' : '▶';
                    
                    html += `
                        <div style="margin-bottom: 2px;">
                            <div onclick="toggleSchema('${schema}')" style="display: flex; align-items: center; padding: 4px 8px; cursor: pointer; border-radius: 4px; hover: background-color: #f0f0f0;" onmouseover="this.style.backgroundColor='#f0f0f0'" onmouseout="this.style.backgroundColor='transparent'">
                                <span style="margin-right: 8px; font-size: 12px; color: #666; width: 12px; text-align: center;">${expandIcon}</span>
                                <span style="color: #1976d2; font-weight: 500; font-size: 14px;">${schema}</span>
                            </div>
                    `;
                    
                    if (isExpanded && window.dbExplorer.schemaData[schema]) {
                        html += '<div style="margin-left: 20px; border-left: 1px solid #e0e0e0; padding-left: 8px;">';
                        
                        window.dbExplorer.schemaData[schema].tables.forEach(table => {
                            const tableKey = `${schema}.${table}`;
                            const isTableExpanded = window.dbExplorer.expandedTables.has(tableKey);
                            const tableExpandIcon = isTableExpanded ? '▼' : '▶';
                            
                            html += `
                                <div style="margin-bottom: 2px;">
                                    <div onclick="toggleTable('${schema}', '${table}')" style="display: flex; align-items: center; padding: 4px 8px; cursor: pointer; border-radius: 4px;" onmouseover="this.style.backgroundColor='#f0f0f0'" onmouseout="this.style.backgroundColor='transparent'">
                                        <span style="margin-right: 8px; font-size: 12px; color: #666; width: 12px; text-align: center;">${tableExpandIcon}</span>
                                        <span style="color: #388e3c; font-size: 14px;">${table}</span>
                                    </div>
                            `;
                            
                            if (isTableExpanded && window.dbExplorer.schemaData[schema].columns[table]) {
                                html += '<div style="margin-left: 20px; border-left: 1px solid #e0e0e0; padding-left: 8px;">';
                                
                                window.dbExplorer.schemaData[schema].columns[table].forEach(([columnName, dataType, nullable]) => {
                                    const notNullBadge = nullable === 'NO' ? '<span style="background: #fff3cd; color: #856404; padding: 1px 4px; border-radius: 2px; font-size: 10px; margin-left: 8px; font-weight: 500;">NOT NULL</span>' : '';
                                    
                                    html += `
                                        <div style="display: flex; align-items: center; padding: 2px 8px; margin-bottom: 1px;">
                                            <span style="margin-right: 8px; width: 12px;"></span>
                                            <span style="color: #424242; font-size: 13px; margin-right: 12px; min-width: 120px;">${columnName}</span>
                                            <span style="color: #757575; font-size: 12px; font-style: italic; margin-right: 8px;">${dataType}</span>
                                            ${notNullBadge}
                                        </div>
                                    `;
                                });
                                
                                html += '</div>';
                            }
                            
                            html += '</div>';
                        });
                        
                        html += '</div>';
                    }
                    
                    html += '</div>';
                });
                
                container.innerHTML = html;
            }
        </script>
    ''')

if __name__ in {"__main__", "__mp_main__"}:
    print("🗄️ Database Explorer")
    print("📦 Install: pip install nicegui psycopg2-binary")
    print("🚀 Starting server on http://localhost:8080")
    
    ui.run(
        title="Database Explorer",
        favicon="🗄️",
        port=8080,
        reload=False,
        show=True
    )