#!/usr/bin/env python3
"""
Efficient PostgreSQL Database Explorer using NiceGUI
Single file implementation with lazy loading and connection pooling
"""

import asyncio
import asyncpg
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import logging

from nicegui import ui, app
from nicegui.events import ValueChangeEventArguments

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ConnectionConfig:
    name: str
    host: str
    port: int
    database: str
    username: str
    password: str
    
    def to_dict(self):
        return asdict(self)

@dataclass 
class DatabaseNode:
    id: str
    name: str
    type: str  # connection, database, schema, table, view, function
    parent_id: Optional[str]
    children: List['DatabaseNode'] = None
    loaded: bool = False
    metadata: Dict = None
    
    def __post_init__(self):
        if self.children is None:
            self.children = []
        if self.metadata is None:
            self.metadata = {}

class ConnectionPool:
    def __init__(self):
        self.pools: Dict[str, asyncpg.Pool] = {}
    
    async def get_pool(self, config: ConnectionConfig) -> asyncpg.Pool:
        key = f"{config.host}:{config.port}:{config.database}"
        if key not in self.pools:
            try:
                self.pools[key] = await asyncpg.create_pool(
                    host=config.host,
                    port=config.port,
                    database=config.database,
                    user=config.username,
                    password=config.password,
                    min_size=1,
                    max_size=5,
                    command_timeout=30
                )
                logger.info(f"Created connection pool for {key}")
            except Exception as e:
                logger.error(f"Failed to create pool for {key}: {e}")
                raise
        return self.pools[key]
    
    async def close_all(self):
        for pool in self.pools.values():
            await pool.close()
        self.pools.clear()

class DatabaseExplorer:
    def __init__(self):
        self.connection_pool = ConnectionPool()
        self.connections: Dict[str, ConnectionConfig] = {}
        self.nodes: Dict[str, DatabaseNode] = {}
        self.tree = None
        self.content_area = None
        self.status_label = None
        
    def generate_node_id(self, parent_id: str, name: str, node_type: str) -> str:
        return f"{parent_id}/{node_type}/{name}" if parent_id else f"{node_type}/{name}"
    
    def build_tree_options(self) -> List[dict]:
        """Build tree options from nodes - only root nodes are returned directly"""
        tree_options = []
        
        # Find root nodes (connections)
        for node_id, node in self.nodes.items():
            if node.parent_id is None:
                tree_options.append(self.build_tree_node_recursive(node_id))
        
        return tree_options
    
    def build_tree_node_recursive(self, node_id: str) -> dict:
        """Recursively build tree node structure"""
        if node_id not in self.nodes:
            return None
            
        node = self.nodes[node_id]
        
        tree_node = {
            'id': node_id,
            'label': node.name,
            'icon': self.get_node_icon(node.type),
        }
        
        # Find direct children
        children = []
        for child_node_id, child_node in self.nodes.items():
            if child_node.parent_id == node_id:
                child_tree_node = self.build_tree_node_recursive(child_node_id)
                if child_tree_node:
                    children.append(child_tree_node)
        
        if children:
            tree_node['children'] = children
        elif not node.loaded and node.type in ['connection', 'database', 'schema', 'table', 'view']:
            # Add dummy child to make node expandable
            tree_node['children'] = [{'id': f'{node_id}_loading', 'label': 'Loading...', 'icon': 'hourglass_empty'}]
        
        return tree_node
    
    def update_tree(self):
        """Update the tree widget with current nodes"""
        if self.tree:
            tree_options = self.build_tree_options()
            self.tree.options = tree_options
            self.tree.update()
    
    async def test_connection(self, config: ConnectionConfig) -> bool:
        try:
            pool = await self.connection_pool.get_pool(config)
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    async def add_connection(self, config: ConnectionConfig):
        if await self.test_connection(config):
            conn_id = f"conn/{config.name}"
            self.connections[conn_id] = config
            
            # Create root node
            node = DatabaseNode(
                id=conn_id,
                name=config.name,
                type="connection",
                parent_id=None,
                metadata={"config": config.to_dict()}
            )
            self.nodes[conn_id] = node
            
            # Update tree
            self.update_tree()
            
            ui.notify(f"Connected to {config.name}", type="positive")
            return True
        else:
            ui.notify(f"Failed to connect to {config.name}", type="negative")
            return False
    
    async def load_databases(self, conn_id: str) -> List[DatabaseNode]:
        config = self.connections[conn_id]
        pool = await self.connection_pool.get_pool(config)
        
        async with pool.acquire() as conn:
            # Get all databases user has access to
            query = """
            SELECT datname 
            FROM pg_database 
            WHERE datallowconn = true 
            AND datname NOT IN ('template0', 'template1')
            ORDER BY datname
            """
            rows = await conn.fetch(query)
            
        databases = []
        for row in rows:
            db_name = row['datname']
            node_id = self.generate_node_id(conn_id, db_name, "database")
            
            node = DatabaseNode(
                id=node_id,
                name=db_name,
                type="database",
                parent_id=conn_id,
                metadata={"database": db_name}
            )
            databases.append(node)
            self.nodes[node_id] = node
            
        return databases
    
    async def load_schemas(self, db_node_id: str) -> List[DatabaseNode]:
        # Get connection config and create new pool for specific database
        parent_conn_id = db_node_id.split('/')[0] + '/' + db_node_id.split('/')[1]
        base_config = self.connections[parent_conn_id]
        db_name = self.nodes[db_node_id].name
        
        # Create config for specific database
        db_config = ConnectionConfig(
            name=f"{base_config.name}_{db_name}",
            host=base_config.host,
            port=base_config.port,
            database=db_name,
            username=base_config.username,
            password=base_config.password
        )
        
        try:
            pool = await self.connection_pool.get_pool(db_config)
            async with pool.acquire() as conn:
                query = """
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                ORDER BY schema_name
                """
                rows = await conn.fetch(query)
                
            schemas = []
            for row in rows:
                schema_name = row['schema_name']
                node_id = self.generate_node_id(db_node_id, schema_name, "schema")
                
                node = DatabaseNode(
                    id=node_id,
                    name=schema_name,
                    type="schema",
                    parent_id=db_node_id,
                    metadata={"database": db_name, "schema": schema_name}
                )
                schemas.append(node)
                self.nodes[node_id] = node
                
            return schemas
        except Exception as e:
            logger.error(f"Failed to load schemas for {db_name}: {e}")
            return []
    
    async def load_tables(self, schema_node_id: str) -> List[DatabaseNode]:
        node = self.nodes[schema_node_id]
        db_name = node.metadata["database"]
        schema_name = node.metadata["schema"]
        
        # Get connection config for this database
        parent_conn_id = schema_node_id.split('/')[0] + '/' + schema_node_id.split('/')[1]
        base_config = self.connections[parent_conn_id]
        
        db_config = ConnectionConfig(
            name=f"{base_config.name}_{db_name}",
            host=base_config.host,
            port=base_config.port,
            database=db_name,
            username=base_config.username,
            password=base_config.password
        )
        
        try:
            pool = await self.connection_pool.get_pool(db_config)
            async with pool.acquire() as conn:
                # Get tables and views
                query = """
                SELECT table_name, table_type
                FROM information_schema.tables 
                WHERE table_schema = $1
                ORDER BY table_type, table_name
                """
                rows = await conn.fetch(query, schema_name)
                
            objects = []
            for row in rows:
                obj_name = row['table_name']
                obj_type = "table" if row['table_type'] == 'BASE TABLE' else "view"
                node_id = self.generate_node_id(schema_node_id, obj_name, obj_type)
                
                node = DatabaseNode(
                    id=node_id,
                    name=obj_name,
                    type=obj_type,
                    parent_id=schema_node_id,
                    metadata={
                        "database": db_name, 
                        "schema": schema_name,
                        "table": obj_name,
                        "table_type": obj_type
                    }
                )
                objects.append(node)
                self.nodes[node_id] = node
                
            return objects
        except Exception as e:
            logger.error(f"Failed to load tables for {schema_name}: {e}")
            return []
    
    async def load_columns(self, table_node_id: str) -> List[DatabaseNode]:
        node = self.nodes[table_node_id]
        db_name = node.metadata["database"]
        schema_name = node.metadata["schema"]
        table_name = node.metadata["table"]
        
        # Get connection config for this database
        parent_conn_id = table_node_id.split('/')[0] + '/' + table_node_id.split('/')[1]
        base_config = self.connections[parent_conn_id]
        
        db_config = ConnectionConfig(
            name=f"{base_config.name}_{db_name}",
            host=base_config.host,
            port=base_config.port,
            database=db_name,
            username=base_config.username,
            password=base_config.password
        )
        
        try:
            pool = await self.connection_pool.get_pool(db_config)
            async with pool.acquire() as conn:
                query = """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
                """
                rows = await conn.fetch(query, schema_name, table_name)
                
            columns = []
            for row in rows:
                col_name = row['column_name']
                node_id = self.generate_node_id(table_node_id, col_name, "column")
                
                node = DatabaseNode(
                    id=node_id,
                    name=f"{col_name} ({row['data_type']})",
                    type="column",
                    parent_id=table_node_id,
                    metadata={
                        "database": db_name,
                        "schema": schema_name,
                        "table": table_name,
                        "column": col_name,
                        "data_type": row['data_type'],
                        "nullable": row['is_nullable'],
                        "default": row['column_default']
                    }
                )
                columns.append(node)
                self.nodes[node_id] = node
                
            return columns
        except Exception as e:
            logger.error(f"Failed to load columns for {table_name}: {e}")
            return []
    
    async def load_table_data(self, table_node_id: str, limit: int = 100):
        node = self.nodes[table_node_id]
        db_name = node.metadata["database"]
        schema_name = node.metadata["schema"]
        table_name = node.metadata["table"]
        
        # Get connection config for this database
        parent_conn_id = table_node_id.split('/')[0] + '/' + table_node_id.split('/')[1]
        base_config = self.connections[parent_conn_id]
        
        db_config = ConnectionConfig(
            name=f"{base_config.name}_{db_name}",
            host=base_config.host,
            port=base_config.port,
            database=db_name,
            username=base_config.username,
            password=base_config.password
        )
        
        try:
            pool = await self.connection_pool.get_pool(db_config)
            async with pool.acquire() as conn:
                # Get column info first
                col_query = """
                SELECT column_name, data_type
                FROM information_schema.columns 
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
                """
                columns = await conn.fetch(col_query, schema_name, table_name)
                
                # Get data
                data_query = f'SELECT * FROM "{schema_name}"."{table_name}" LIMIT $1'
                rows = await conn.fetch(data_query, limit)
                
                # Get row count
                count_query = f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"'
                total_count = await conn.fetchval(count_query)
                
            return {
                'columns': [{'name': col['column_name'], 'type': col['data_type']} for col in columns],
                'rows': [dict(row) for row in rows],
                'total_count': total_count
            }
        except Exception as e:
            logger.error(f"Failed to load data for {table_name}: {e}")
            return None
    
    async def handle_tree_expand(self, e):
        node_id = e.value
        logger.info(f"Expanding node: {node_id}")
        
        # Skip loading/dummy nodes
        if node_id.endswith('_loading') or node_id.endswith('_dummy'):
            return
            
        if node_id not in self.nodes:
            logger.warning(f"Node not found: {node_id}")
            return
            
        node = self.nodes[node_id]
        if node.loaded:
            logger.info(f"Node already loaded: {node_id}")
            return
            
        self.status_label.text = f"Loading {node.name}..."
        
        try:
            children = []
            if node.type == "connection":
                children = await self.load_databases(node_id)
            elif node.type == "database":
                children = await self.load_schemas(node_id)
            elif node.type == "schema":
                children = await self.load_tables(node_id)
            elif node.type in ["table", "view"]:
                children = await self.load_columns(node_id)
            
            # Mark node as loaded
            node.loaded = True
            node.children = children
            
            # Update tree display
            self.update_tree()
            
            self.status_label.text = f"Loaded {len(children)} items for {node.name}"
            logger.info(f"Loaded {len(children)} children for {node_id}")
            
        except Exception as e:
            logger.error(f"Failed to load children for {node_id}: {e}")
            self.status_label.text = f"Error loading {node.name}: {str(e)}"
            ui.notify(f"Error loading {node.name}: {str(e)}", type="negative")
    
    def get_node_icon(self, node_type: str) -> str:
        icons = {
            "connection": "dns",
            "database": "storage",
            "schema": "folder",
            "table": "table_chart",
            "view": "visibility",
            "column": "view_column"
        }
        return icons.get(node_type, "help")
    
    async def handle_tree_select(self, e):
        node_id = e.value
        
        # Skip loading/dummy nodes
        if node_id.endswith('_loading') or node_id.endswith('_dummy'):
            return
            
        if node_id not in self.nodes:
            return
            
        node = self.nodes[node_id]
        
        # Clear content area
        self.content_area.clear()
        
        with self.content_area:
            ui.label(f"{node.type.title()}: {node.name}").classes('text-h6 mb-4')
            
            if node.type in ["table", "view"]:
                # Show table data
                await self.show_table_data(node_id)
            else:
                # Show metadata
                self.show_node_metadata(node)
    
    async def show_table_data(self, table_node_id: str):
        self.status_label.text = "Loading table data..."
        
        data = await self.load_table_data(table_node_id, limit=100)
        
        if data:
            with ui.row().classes('w-full mb-4'):
                ui.label(f"Showing {len(data['rows'])} of {data['total_count']} rows")
                ui.button("Refresh", icon="refresh", on_click=lambda: asyncio.create_task(self.show_table_data(table_node_id)))
            
            if data['rows']:
                # Create table
                columns = [{'name': col['name'], 'label': col['name'], 'field': col['name']} for col in data['columns']]
                ui.table(columns=columns, rows=data['rows']).classes('w-full')
            else:
                ui.label("No data found")
                
            self.status_label.text = f"Loaded {len(data['rows'])} rows"
        else:
            ui.label("Failed to load table data")
            self.status_label.text = "Error loading table data"
    
    def refresh_tree(self):
        """Refresh the entire tree"""
        self.update_tree()
        ui.notify("Tree refreshed", type="positive")
    
    def show_node_metadata(self, node: DatabaseNode):
        if node.metadata:
            ui.label("Metadata:").classes('font-bold mb-2')
            for key, value in node.metadata.items():
                if key != 'config':  # Don't show connection config
                    ui.label(f"{key}: {value}")
    
    def setup_ui(self):
        with ui.splitter(value=20).classes('w-full h-screen') as splitter:
            with splitter.before:
                with ui.column().classes('w-full h-full p-4'):
                    ui.label('Database Explorer').classes('text-h5 mb-4')
                    
                    # Connection form
                    with ui.expansion('Add Connection', icon='add').classes('w-full mb-4'):
                        self.setup_connection_form()
                    
                    # Refresh button
                    ui.button('Refresh All', icon='refresh', on_click=self.refresh_tree).classes('w-full mb-2')
                    
                    # Tree
                    self.tree = ui.tree(
                        [],
                        label_key='label',
                        on_expand=self.handle_tree_expand,
                        on_select=self.handle_tree_select
                    ).classes('w-full')
            
            with splitter.after:
                with ui.column().classes('w-full h-full p-4'):
                    self.content_area = ui.column().classes('w-full flex-1')
                    
                    # Status bar
                    with ui.row().classes('w-full mt-auto pt-4 border-t'):
                        self.status_label = ui.label('Ready').classes('text-sm text-gray-600')
    
    def setup_connection_form(self):
        with ui.column().classes('w-full gap-2'):
            name_input = ui.input('Connection Name', placeholder='My Database').classes('w-full')
            host_input = ui.input('Host', placeholder='localhost').classes('w-full')
            port_input = ui.number('Port', value=5432).classes('w-full')
            database_input = ui.input('Database', placeholder='postgres').classes('w-full')
            username_input = ui.input('Username', placeholder='postgres').classes('w-full')
            password_input = ui.input('Password', password=True).classes('w-full')
            
            async def connect():
                config = ConnectionConfig(
                    name=name_input.value or 'Unnamed',
                    host=host_input.value or 'localhost',
                    port=int(port_input.value or 5432),
                    database=database_input.value or 'postgres',
                    username=username_input.value or 'postgres',
                    password=password_input.value or ''
                )
                
                success = await self.add_connection(config)
                if success:
                    # Clear form
                    name_input.value = ''
                    host_input.value = ''
                    port_input.value = 5432
                    database_input.value = ''
                    username_input.value = ''
                    password_input.value = ''
            
            ui.button('Connect', on_click=connect, icon='connect_without_contact').classes('w-full')

# Global explorer instance
explorer = DatabaseExplorer()

@app.on_startup
async def startup():
    logger.info("Starting PostgreSQL Database Explorer")

@app.on_shutdown
async def shutdown():
    logger.info("Shutting down...")
    await explorer.connection_pool.close_all()

# Main page
@ui.page('/')
async def main_page():
    ui.page_title('PostgreSQL Database Explorer')
    explorer.setup_ui()

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(
        title='PostgreSQL Database Explorer',
        port=8080,
        show=True,
        reload=False
    )