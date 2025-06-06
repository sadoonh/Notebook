import asyncpg
from nicegui import ui

db_pool = None

def build_schema_tree_nodes(schema_data: dict) -> list:
    """Build tree nodes for database schema with aligned column types."""
    # Find the maximum column name length across all schemas/tables
    max_col_length = 0
    for schema_name, tables in schema_data.items():
        for table_name, columns in tables.items():
            for col_name, _ in columns:
                max_col_length = max(max_col_length, len(col_name))
    
    # Add some padding
    max_col_length += 1
    
    nodes = []
    for schema_name, tables in schema_data.items():
        # Create table nodes for this schema
        table_nodes = []
        for table_name, columns in tables.items():
            # Create column nodes for this table
            column_nodes = []
            for i, (col_name, col_type) in enumerate(columns):
                column_nodes.append({
                    'id': f'col_{schema_name}_{table_name}_{i}',
                    'label': f'{col_name.ljust(max_col_length)} {col_type}',
                    'col_type': col_type,
                    'col_name': col_name,
                    'is_column': True,
                    'max_width': max_col_length,
                    'header': 'standard',
                    'selectable': False
                })
            
            # Create table node
            table_nodes.append({
                'id': f'tbl_{schema_name}_{table_name}',
                'label': table_name,
                'icon': 'table_view',
                'text_color': 'primary',
                'children': column_nodes,
                'header': 'standard',
                'selectable': False,
                'is_column': False,
                'is_table': True
            })
        
        # Create schema node
        nodes.append({
            'id': f'schema_{schema_name}',
            'label': schema_name,
            'icon': 'folder',
            'text_color': 'secondary',
            'children': table_nodes,
            'header': 'standard',
            'selectable': False,
            'is_column': False,
            'is_table': False,
            'is_schema': True
        })
    
    return nodes

@ui.page('/')
async def main_page():
    ui.label('Database Schema Browser').style('font-size: 24px; font-weight: bold')
    
    # Connection inputs
    with ui.card().style('padding: 20px; margin-bottom: 20px'):
        ui.label('Database Connection').style('font-size: 18px; font-weight: bold; margin-bottom: 10px')
        
        with ui.row():
            host = ui.input('Host', value='localhost').style('width: 150px')
            port = ui.input('Port', value='5432').style('width: 100px')
            database = ui.input('Database', value='postgres').style('width: 150px')
        
        with ui.row():
            user = ui.input('User', value='postgres').style('width: 150px')
            password = ui.input('Password', password=True).style('width: 150px')
            load_btn = ui.button('Load Schema', 
                               on_click=lambda: load_schema(host.value, port.value, database.value, user.value, password.value),
                               color='primary')
    
    # Tree container
    tree_container = ui.column()
    status_label = ui.label('')
    
    async def load_schema(host_val, port_val, db_val, user_val, pass_val):
        global db_pool
        
        status_label.text = 'Connecting...'
        status_label.style('color: orange')
        
        try:
            # Connect to database
            db_pool = await asyncpg.create_pool(
                host=host_val, 
                port=int(port_val), 
                database=db_val, 
                user=user_val, 
                password=pass_val,
                min_size=1,
                max_size=5
            )
            
            status_label.text = 'Loading schema...'
            
            # Get schema data using your query
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT table_schema, table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                    ORDER BY table_schema, table_name, ordinal_position
                """)
            
            # Organize data by schema -> table -> columns
            schema_data = {}
            for row in rows:
                schema = row['table_schema']
                table = row['table_name']
                column = row['column_name']
                data_type = row['data_type']
                
                if schema not in schema_data:
                    schema_data[schema] = {}
                if table not in schema_data[schema]:
                    schema_data[schema][table] = []
                
                schema_data[schema][table].append((column, data_type))
            
            # Build tree nodes
            tree_nodes = build_schema_tree_nodes(schema_data)
            
            # Clear and rebuild tree
            tree_container.clear()
            with tree_container:
                current_schema_tree = ui.tree(
                    nodes=tree_nodes,
                    node_key='id',
                    label_key='label',
                    children_key='children'
                ).style('font-family: monospace; font-size: 14px')
            
            # Update status
            total_schemas = len(schema_data)
            total_tables = sum(len(tables) for tables in schema_data.values())
            total_columns = sum(len(columns) for tables in schema_data.values() for columns in tables.values())
            
            status_label.text = f'Loaded: {total_schemas} schemas, {total_tables} tables, {total_columns} columns'
            status_label.style('color: green')
            
        except Exception as e:
            status_label.text = f'Error: {str(e)}'
            status_label.style('color: red')

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(title='Database Schema Browser', port=8080)