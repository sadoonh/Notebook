import psycopg2
from nicegui import ui

# --- Database Configuration ---
# IMPORTANT: Never hardcode sensitive credentials in production.
# Use environment variables, a config file, or a secrets management system.
DB_CONFIG = {
    'host': 'localhost',
    'database': 'Test',
    'user': 'postgres',
    'password': 'Sa3dan123',
    'port': 5432 # Default PostgreSQL port
}

# --- Database Interaction Function ---
async def get_database_schema():
    """
    Connects to the PostgreSQL database and retrieves all table names
    along with their respective column names and data types.
    Converts 'character varying' data type to 'varchar'.
    """
    schema_data = {}
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        tables = [row[0] for row in cur.fetchall()]

        for table_name in tables:
            schema_data[table_name] = []
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position;
            """, (table_name,))

            columns_with_types = cur.fetchall()

            processed_columns = []
            for col_name, col_type in columns_with_types:
                if col_type == 'character varying':
                    col_type = 'varchar'
                elif col_type == 'timestamp without time zone':
                    col_type = 'timestamp'
                processed_columns.append((col_name, col_type))

            schema_data[table_name] = processed_columns

        return schema_data

    except psycopg2.Error as e:
        print(f"Database connection or query error: {e}")
        return {"error": f"Failed to connect or query database: {e}"}
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {"error": f"An unexpected error occurred: {e}"}
    finally:
        if conn:
            cur.close()
            conn.close()

# --- Helper Function to Build Tree Nodes (MODIFIED) ---
def build_tree_nodes(schema_data: dict) -> list:
    # First, find the maximum column name length across all tables
    max_col_length = 0
    for table_name, columns in schema_data.items():
        for col_name, _ in columns:
            max_col_length = max(max_col_length, len(col_name))
    
    # Add some padding (in characters)
    max_col_length += 2
    
    nodes = []
    for table_name, columns in schema_data.items():
        # Create child nodes for columns
        column_nodes = []
        for i, (col_name, col_type) in enumerate(columns):
            column_nodes.append({
                'id': f'col_{table_name}_{i}',
                'label': col_name,  # Just the column name
                'col_type': col_type,  # Store type separately
                'is_column': True,  # Flag to identify column nodes
                'max_width': max_col_length,  # Pass the max width
                'header': 'standard',
                'selectable': False
            })

        # Create parent node for the table
        nodes.append({
            'id': f'tbl_{table_name}',
            'label': table_name,
            'icon': 'backup_table',
            'text_color': 'primary',
            'children': column_nodes,
            'header': 'standard',
            'selectable': False,
            'is_column': False
        })
    return nodes


# --- NiceGUI UI Setup (MODIFIED to use ui.tree with custom slot) ---
@ui.page('/')
async def index_page():
    # Main content area
    ui.label('Welcome to the Database Schema Browser').classes('text-3xl font-bold p-4')
    ui.label('Click the menu icon to open the left drawer and view tables, columns, and their types.').classes('p-4')
    ui.icon('menu', size='lg').classes('absolute-top-left m-4 cursor-pointer').on('click', ui.left_drawer.toggle)

    # Left Drawer
    with ui.left_drawer().props('bordered') as left_drawer:
        ui.label('Database Schema').classes('text-lg font-semibold p-4')
        ui.separator()

        schema_data = await get_database_schema()

        if "error" in schema_data:
            ui.label(schema_data["error"]).classes('text-red-500 p-4')
        elif not schema_data:
            ui.label("No tables found in the 'public' schema.").classes('text-gray-500 p-4')
        else:
            # Create a placeholder for buttons
            button_container = ui.row().classes('w-full px-4 pb-2 gap-2')
            
            tree_nodes = build_tree_nodes(schema_data)
            
            # Create tree with custom label slot
            tree = ui.tree(nodes=tree_nodes,
                          node_key='id',
                          label_key='label',
                          children_key='children') \
                .classes('w-full')
            
            # Now add buttons to the container
            with button_container:
                ui.button('Expand All', icon='unfold_more', on_click=lambda: tree.run_method('expandAll')) \
                    .props('flat dense color=primary size=sm') \
                    .classes('text-s')
                ui.button('Collapse All', icon='unfold_less', on_click=lambda: tree.run_method('collapseAll')) \
                    .props('flat dense color=primary size=sm') \
                    .classes('text-s')
            
            # Custom slot for rendering labels
            tree.add_slot('default-header', '''
                <span v-if="props.node.is_column" style="display: flex; align-items: center; font-family: monospace;">
                    <span :style="`display: inline-block; width: ${props.node.max_width}ch;`">{{ props.node.label }}</span>
                    <span style="color: #9e9e9e;">{{ props.node.col_type }}</span>
                </span>
                <span v-else style="display: flex; align-items: center;">
                    <q-icon :name="props.node.icon" style="margin-right: 8px;" />
                    <span>{{ props.node.label }}</span>
                </span>
            ''')

# --- Run NiceGUI Application ---
ui.run()