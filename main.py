import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import sqlite3
import csv
from datetime import datetime
import re
import os
import json
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm


class DatabaseManager:
    """Gerencia todas as operações com o banco de dados SQLite"""
    
    def __init__(self, db_name="dados_cte.db"):
        self.db_name = db_name
        self.conn = None
        self.cursor = None
        self.connect()
        self.create_table()
    
    def connect(self):
        """Conecta ao banco de dados"""
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
    
    def create_table(self):
        """Cria a tabela principal se não existir"""
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS cte_data (
                numero_cte TEXT PRIMARY KEY,
                emissao_cte TEXT,
                notas TEXT,
                remetente TEXT,
                destinatario TEXT,
                cidade_destino_cte TEXT,
                representante_entrega TEXT,
                filial_resp_entrega TEXT,
                status_entrega_tela_sac TEXT,
                data_chegada TEXT,
                vendedor TEXT,
                previsao_entrega TEXT,
                ultima_ocorrencia TEXT
            )
        ''')
        self.conn.commit()
    
    def insert_row(self, data_dict):
        """Insere uma linha no banco de dados"""
        try:
            self.cursor.execute('''
                INSERT INTO cte_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data_dict.get('numero_cte', ''),
                data_dict.get('emissao_cte', ''),
                data_dict.get('notas', ''),
                data_dict.get('remetente', ''),
                data_dict.get('destinatario', ''),
                data_dict.get('cidade_destino_cte', ''),
                data_dict.get('representante_entrega', ''),
                data_dict.get('filial_resp_entrega', ''),
                data_dict.get('status_entrega_tela_sac', ''),
                data_dict.get('data_chegada', ''),
                data_dict.get('vendedor', ''),
                data_dict.get('previsao_entrega', ''),
                data_dict.get('ultima_ocorrencia', '')
            ))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # CTe já existe
    
    def cte_exists(self, numero_cte):
        """Verifica se um CTe já existe no banco"""
        self.cursor.execute('SELECT numero_cte FROM cte_data WHERE numero_cte = ?', (numero_cte,))
        return self.cursor.fetchone() is not None
    
    def get_all_data(self):
        """Retorna todos os dados do banco"""
        self.cursor.execute('SELECT * FROM cte_data')
        return self.cursor.fetchall()
    
    def search_data(self, column, search_term):
        """Busca dados em uma coluna específica ou em todas"""
        if column == "Todas":
            # Busca em todas as colunas
            query = '''
                SELECT * FROM cte_data WHERE 
                emissao_cte LIKE ? OR
                numero_cte LIKE ? OR
                notas LIKE ? OR
                remetente LIKE ? OR
                destinatario LIKE ? OR
                cidade_destino_cte LIKE ? OR
                representante_entrega LIKE ? OR
                filial_resp_entrega LIKE ? OR
                status_entrega_tela_sac LIKE ? OR
                data_chegada LIKE ? OR
                vendedor LIKE ? OR
                previsao_entrega LIKE ? OR
                ultima_ocorrencia LIKE ?
            '''
            params = tuple([f'%{search_term}%'] * 13)
        else:
            # Mapeia nome da coluna para nome no banco
            column_map = {
                'Emissão CT-e': 'emissao_cte',
                'Numero CT-e': 'numero_cte',
                'Notas': 'notas',
                'Remetente': 'remetente',
                'Destinatário': 'destinatario',
                'Cidade Destino CT-e': 'cidade_destino_cte',
                'Representante Entrega': 'representante_entrega',
                'Filial Resp. Entrega': 'filial_resp_entrega',
                'Status Entrega Tela SAC': 'status_entrega_tela_sac',
                'Data Chegada': 'data_chegada',
                'Vendedor': 'vendedor',
                'Previsão Entrega': 'previsao_entrega',
                'Última Ocorrência': 'ultima_ocorrencia'
            }
            db_column = column_map.get(column, 'numero_cte')
            query = f'SELECT * FROM cte_data WHERE {db_column} LIKE ?'
            params = (f'%{search_term}%',)
        
        self.cursor.execute(query, params)
        return self.cursor.fetchall()
    
    def update_cell(self, numero_cte, column_name, new_value):
        """Atualiza o valor de uma célula específica"""
        column_map = {
            'Emissão CT-e': 'emissao_cte',
            'Numero CT-e': 'numero_cte',
            'Notas': 'notas',
            'Remetente': 'remetente',
            'Destinatário': 'destinatario',
            'Cidade Destino CT-e': 'cidade_destino_cte',
            'Representante Entrega': 'representante_entrega',
            'Filial Resp. Entrega': 'filial_resp_entrega',
            'Status Entrega Tela SAC': 'status_entrega_tela_sac',
            'Data Chegada': 'data_chegada',
            'Vendedor': 'vendedor',
            'Previsão Entrega': 'previsao_entrega',
            'Última Ocorrência': 'ultima_ocorrencia'
        }
        
        db_column = column_map.get(column_name)
        if db_column and db_column != 'numero_cte':  # Não permite editar a chave primária
            self.cursor.execute(f'UPDATE cte_data SET {db_column} = ? WHERE numero_cte = ?', 
                              (new_value, numero_cte))
            self.conn.commit()
            return True
        return False
    
    def close(self):
        """Fecha a conexão com o banco"""
        if self.conn:
            self.conn.close()


class SpreadsheetApp:
    """Aplicação principal com interface tipo planilha"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("Sistema de Gestão de CT-e")
        self.root.geometry("1400x700")
        
        # Colunas padrão
        self.original_columns = [
            'Emissão CT-e', 'Numero CT-e', 'Notas', 'Remetente', 'Destinatário',
            'Cidade Destino CT-e', 'Representante Entrega', 'Filial Resp. Entrega',
            'Status Entrega Tela SAC', 'Data Chegada', 'Vendedor', 
            'Previsão Entrega', 'Última Ocorrência'
        ]
        
        # Labels editáveis das colunas
        self.column_labels = self.original_columns.copy()
        
        # Colunas visíveis (todas visíveis por padrão)
        self.visible_columns = {col: True for col in self.original_columns}
        
        # Inicializa banco de dados
        self.db = DatabaseManager()
        
        # Filtros
        self.global_filters = {}  # Formato: {coluna: valor}
        self.local_filters = {}   # Formato: {coluna: {"type": "contains"|"not_contains", "value": valor}}
        self.config_file = 'config.json'
        
        # Highlights de linhas (chave: numero_cte, valor: cor)
        self.highlights = {}
        
        # Cores disponíveis para highlight
        self.highlight_colors = {
            'Amarelo': '#FFFF99',
            'Verde': '#99FF99',
            'Vermelho': '#FF9999'
        }
        
        # Ordenação de colunas (lista ordenada de tuplas: [(coluna, direção), ...])
        # direção pode ser 'asc' ou 'desc'
        self.sort_columns = []
        
        # Carrega configurações salvas (filtros, colunas visíveis e highlights)
        self.load_config()
        
        # Estado de busca expandida
        self.search_expanded = False
        
        self.setup_ui()
        self.load_data()
    
    def setup_ui(self):
        """Configura toda a interface do usuário"""
        
        # Frame superior - Botões e controles
        top_frame = ttk.Frame(self.root, padding="10")
        top_frame.pack(fill=tk.X, side=tk.TOP)
        
        # Botões principais
        ttk.Button(top_frame, text="Importar CSV", command=self.import_csv).pack(side=tk.LEFT, padx=5)
        ttk.Button(top_frame, text="Configurar Filtro Global", command=self.config_global_filter).pack(side=tk.LEFT, padx=5)
        ttk.Button(top_frame, text="Limpar Filtros Locais", command=self.clear_local_filters).pack(side=tk.LEFT, padx=5)
        ttk.Button(top_frame, text="Limpar Ordenações", command=lambda: self.clear_all_sorts()).pack(side=tk.LEFT, padx=5)
        ttk.Button(top_frame, text="Editar Labels", command=self.edit_column_labels).pack(side=tk.LEFT, padx=5)
        ttk.Button(top_frame, text="Destacar Linha", command=self.highlight_row).pack(side=tk.LEFT, padx=5)
        ttk.Button(top_frame, text="Gerar Relatório", command=self.generate_report).pack(side=tk.LEFT, padx=5)
        ttk.Button(top_frame, text="Atualizar", command=self.load_data).pack(side=tk.LEFT, padx=5)
        
        # Frame de busca
        search_frame = ttk.Frame(self.root, padding="10")
        search_frame.pack(fill=tk.X, side=tk.TOP)
        
        ttk.Label(search_frame, text="Busca:").pack(side=tk.LEFT, padx=5)
        
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(search_frame, textvariable=self.search_var, width=40)
        search_entry.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(search_frame, text="Buscar", command=self.perform_search).pack(side=tk.LEFT, padx=5)
        
        # Botão X para limpar busca (inicialmente oculto)
        self.clear_search_button = ttk.Button(search_frame, text="✕", width=3, command=self.clear_search)
        
        ttk.Button(search_frame, text="⚙", width=3, command=self.toggle_search_options).pack(side=tk.LEFT, padx=5)
        
        # Frame de opções de busca (inicialmente oculto)
        self.search_options_frame = ttk.Frame(self.root, padding="5")
        
        ttk.Label(self.search_options_frame, text="Buscar em:").pack(side=tk.LEFT, padx=5)
        
        self.search_column_var = tk.StringVar(value="Todas")
        visible_labels = self.get_visible_labels()
        column_combo = ttk.Combobox(self.search_options_frame, 
                                    textvariable=self.search_column_var,
                                    values=["Todas"] + visible_labels,
                                    state="readonly",
                                    width=30)
        column_combo.pack(side=tk.LEFT, padx=5)
        
        # Frame para a tabela
        table_frame = ttk.Frame(self.root)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Scrollbars
        vsb = ttk.Scrollbar(table_frame, orient="vertical")
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        
        hsb = ttk.Scrollbar(table_frame, orient="horizontal")
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Treeview (tabela) com cabeçalhos nativos
        visible_labels = self.get_visible_labels()
        self.tree = ttk.Treeview(table_frame, 
                                columns=visible_labels,
                                show='headings',
                                yscrollcommand=vsb.set,
                                xscrollcommand=hsb.set)
        
        vsb.config(command=self.tree.yview)
        hsb.config(command=self.tree.xview)
        
        # Configura colunas visíveis com cabeçalhos clicáveis
        self.update_column_headers()
        for col in visible_labels:
            self.tree.column(col, width=120, anchor=tk.W)
        
        # Configura tags de cores para highlights
        for color_name, color_value in self.highlight_colors.items():
            self.tree.tag_configure(color_name, background=color_value)
        
        self.tree.pack(fill=tk.BOTH, expand=True)
        
        # Bind para edição de células
        self.tree.bind('<Double-1>', self.on_double_click)
        
        # Bind para botão direito abre filtro nos cabeçalhos
        self.tree.bind('<Button-3>', self.on_right_click_header)
        
        # Status bar
        self.status_var = tk.StringVar()
        self.status_var.set("Pronto | Clique no cabeçalho para ordenar | Clique direito no cabeçalho para filtrar")
        status_bar = ttk.Label(self.root, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(fill=tk.X, side=tk.BOTTOM)
    
    def toggle_search_options(self):
        """Expande/recolhe as opções de busca"""
        if self.search_expanded:
            self.search_options_frame.pack_forget()
            self.search_expanded = False
        else:
            self.search_options_frame.pack(fill=tk.X, side=tk.TOP, after=self.root.winfo_children()[1])
            self.search_expanded = True
    
    def update_column_headers(self):
        """Atualiza cabeçalhos das colunas com indicadores de ordenação e filtro"""
        visible_labels = self.get_visible_labels()
        for i, col in enumerate(visible_labels):
            display_text = col
            
            # Adiciona indicador de ordenação
            sort_symbol = self.get_sort_symbol(col)
            display_text = f"{display_text} {sort_symbol}"
            
            # Adiciona indicador de filtro
            if col in self.local_filters:
                display_text += " *"
            
            # Configura o cabeçalho com clique simples para ordenação e Shift+clique para filtro
            self.tree.heading(i, text=display_text, 
                            command=lambda c=col: self.on_column_click(c))
    
    def get_sort_symbol(self, column_name):
        """Retorna o símbolo de ordenação para uma coluna"""
        for sort_col, direction in self.sort_columns:
            if sort_col == column_name:
                if direction == 'asc':
                    return '↑'
                else:
                    return '↓'
        return '—'
    
    def on_column_click(self, column_name):
        """Clique no cabeçalho: clique simples cicla ordenação, Shift+clique abre filtro"""
        # Verifica se Shift está pressionado
        if self.root.winfo_containing(self.root.winfo_pointerx(), self.root.winfo_pointery()):
            # Tenta detectar se shift está pressionado através do estado do evento
            # Como não temos acesso direto ao evento aqui, vamos usar um approach diferente
            # Vamos fazer que clique simples ordena
            self.cycle_sort(column_name)
    
    def on_right_click_header(self, event):
        """Clique direito no cabeçalho abre filtro"""
        region = self.tree.identify_region(event.x, event.y)
        if region == "heading":
            column = self.tree.identify_column(event.x)
            col_index = int(column.replace('#', '')) - 1
            visible_labels = self.get_visible_labels()
            if 0 <= col_index < len(visible_labels):
                column_name = visible_labels[col_index]
                self.open_filter_dialog(column_name)
    
    def on_column_right_click(self, event, column_name):
        """Clique direito no cabeçalho abre filtro"""
        self.open_filter_dialog(column_name)
    
    def cycle_sort(self, column_name):
        """Cicla através dos estados de ordenação: — → ↑ → ↓ → —"""
        # Encontra estado atual
        current_state = None
        for sort_col, direction in self.sort_columns:
            if sort_col == column_name:
                current_state = direction
                break
        
        # Remove esta coluna da lista de ordenação
        self.sort_columns = [(col, dir) for col, dir in self.sort_columns if col != column_name]
        
        # Cicla para o próximo estado
        if current_state is None:
            # — → ↑
            self.sort_columns.append((column_name, 'asc'))
        elif current_state == 'asc':
            # ↑ → ↓
            self.sort_columns.append((column_name, 'desc'))
        # Se for 'desc', não adiciona de volta (↓ → —)
        
        # Atualiza cabeçalho
        new_symbol = self.get_sort_symbol(column_name)
        self.update_column_headers()
        
        # Salva e recarrega
        self.save_config()
        self.load_data()
        
        # Atualiza status
        if new_symbol == '↑':
            self.status_var.set(f"Ordenando por '{column_name}' - Crescente")
        elif new_symbol == '↓':
            self.status_var.set(f"Ordenando por '{column_name}' - Decrescente")
        else:
            self.status_var.set(f"Ordenação removida de '{column_name}'")
    
    def open_filter_dialog(self, column_name):
        """Abre diálogo de filtro para uma coluna"""
        dialog = tk.Toplevel(self.root)
        dialog.title(f"Filtrar: {column_name}")
        dialog.geometry("450x350")
        dialog.transient(self.root)
        dialog.grab_set()
        
        ttk.Label(dialog, text=f"Filtro para coluna: {column_name}", font=('', 10, 'bold')).pack(pady=10)
        
        frame = ttk.Frame(dialog, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Tipo de filtro
        ttk.Label(frame, text="Tipo de Filtro:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        filter_type_var = tk.StringVar(value="contains")
        
        type_frame = ttk.Frame(frame)
        type_frame.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        ttk.Radiobutton(type_frame, text="Deve conter", variable=filter_type_var, value="contains").pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(type_frame, text="Não deve conter", variable=filter_type_var, value="not_contains").pack(side=tk.LEFT, padx=5)
        
        # Valor
        ttk.Label(frame, text="Valor:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        value_var = tk.StringVar()
        
        # Se já existe filtro, preenche com valores atuais
        if column_name in self.local_filters:
            value_var.set(self.local_filters[column_name]['value'])
            filter_type_var.set(self.local_filters[column_name]['type'])
        
        value_entry = ttk.Entry(frame, textvariable=value_var, width=30)
        value_entry.grid(row=1, column=1, padx=5, pady=5)
        value_entry.focus()
        
        # Lista de filtros ativos
        ttk.Separator(frame, orient='horizontal').grid(row=2, column=0, columnspan=2, sticky='ew', pady=10)
        ttk.Label(frame, text="Filtros Locais Ativos:", font=('', 9, 'bold')).grid(row=3, column=0, columnspan=2, padx=5, pady=5, sticky=tk.W)
        
        filter_list_frame = ttk.Frame(frame)
        filter_list_frame.grid(row=4, column=0, columnspan=2, padx=5, pady=5, sticky=tk.W)
        
        if self.local_filters:
            filter_text = "\n".join([
                f"• {col}: {'Contém' if cfg['type'] == 'contains' else 'Não contém'} '{cfg['value']}'"
                for col, cfg in self.local_filters.items()
            ])
        else:
            filter_text = "Nenhum filtro ativo"
        
        filter_label = ttk.Label(filter_list_frame, text=filter_text, foreground="blue", justify=tk.LEFT)
        filter_label.pack(anchor=tk.W)
        
        # Botões
        button_frame = ttk.Frame(frame)
        button_frame.grid(row=5, column=0, columnspan=2, pady=15)
        
        def apply_filter():
            val = value_var.get().strip()
            if val:
                self.local_filters[column_name] = {
                    'type': filter_type_var.get(),
                    'value': val
                }
                self.update_filter_indicators()
                self.load_data()
                dialog.destroy()
        
        def remove_filter():
            if column_name in self.local_filters:
                del self.local_filters[column_name]
                self.update_filter_indicators()
                self.load_data()
            dialog.destroy()
        
        ttk.Button(button_frame, text="Aplicar Filtro", command=apply_filter).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Remover Filtro", command=remove_filter).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Cancelar", command=dialog.destroy).pack(side=tk.LEFT, padx=5)
        
        # Enter para aplicar
        value_entry.bind('<Return>', lambda e: apply_filter())
    
    def update_filter_indicators(self):
        """Atualiza os indicadores de filtro nos cabeçalhos"""
        self.update_column_headers()
    
    def load_config(self):
        """Carrega configurações salvas do arquivo (filtros, colunas visíveis, labels e highlights)"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    self.global_filters = config.get('global_filters', {})
                    saved_visible = config.get('visible_columns', {})
                    # Atualiza visible_columns com valores salvos
                    if saved_visible:
                        for col in self.original_columns:
                            if col in saved_visible:
                                self.visible_columns[col] = saved_visible[col]
                    # Carrega labels personalizados
                    saved_labels = config.get('column_labels', [])
                    if saved_labels and len(saved_labels) == len(self.original_columns):
                        self.column_labels = saved_labels
                    # Carrega highlights
                    self.highlights = config.get('highlights', {})
                    # Carrega ordenação
                    self.sort_columns = config.get('sort_columns', [])
        except Exception as e:
            print(f"Erro ao carregar configurações: {e}")
            self.global_filters = {}
    
    def save_config(self):
        """Salva configurações em arquivo (filtros, colunas visíveis, labels e highlights)"""
        try:
            config = {
                'global_filters': self.global_filters,
                'visible_columns': self.visible_columns,
                'column_labels': self.column_labels,
                'highlights': self.highlights,
                'sort_columns': self.sort_columns
            }
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Erro ao salvar configurações: {e}")
    
    def get_visible_labels(self):
        """Retorna lista de labels das colunas visíveis"""
        return [self.column_labels[i] for i, col in enumerate(self.original_columns) 
                if self.visible_columns.get(col, True)]
    
    def get_visible_indices(self):
        """Retorna lista de índices das colunas visíveis"""
        return [i for i, col in enumerate(self.original_columns) 
                if self.visible_columns.get(col, True)]
    
    def recreate_tree(self):
        """Recria a árvore com as colunas visíveis"""
        # Obtém o frame da tabela
        table_frame = self.tree.master
        
        # Remove a árvore antiga
        self.tree.destroy()
        
        # Obtém os scrollbars existentes
        scrollbars = [w for w in table_frame.winfo_children() if isinstance(w, ttk.Scrollbar)]
        vsb = None
        hsb = None
        for sb in scrollbars:
            if sb.cget('orient') == 'vertical':
                vsb = sb
            elif sb.cget('orient') == 'horizontal':
                hsb = sb
        
        # Cria nova árvore com colunas visíveis
        visible_labels = self.get_visible_labels()
        self.tree = ttk.Treeview(table_frame, 
                                columns=visible_labels,
                                show='headings',
                                yscrollcommand=vsb.set if vsb else None,
                                xscrollcommand=hsb.set if hsb else None)
        
        if vsb:
            vsb.config(command=self.tree.yview)
        if hsb:
            hsb.config(command=self.tree.xview)
        
        # Configura colunas visíveis com cabeçalhos
        self.update_column_headers()
        for col in visible_labels:
            self.tree.column(col, width=120, anchor=tk.W)
        
        # Configura tags de cores para highlights
        for color_name, color_value in self.highlight_colors.items():
            self.tree.tag_configure(color_name, background=color_value)
        
        self.tree.pack(fill=tk.BOTH, expand=True)
        
        # Bind para edição de células
        self.tree.bind('<Double-1>', self.on_double_click)
        
        # Bind para botão direito abre filtro nos cabeçalhos
        self.tree.bind('<Button-3>', self.on_right_click_header)
        
        # Recarrega dados
        self.load_data()
    
    def clear_all_sorts(self, dialog=None):
        """Limpa todas as ordenações"""
        self.sort_columns = []
        self.save_config()
        self.update_column_headers()
        self.load_data()
        if dialog:
            dialog.destroy()
    
    def load_data(self):
        """Carrega dados do banco, aplica filtros locais e ordenação"""
        # Limpa a tabela
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Carrega todos os dados
        data = self.db.get_all_data()
        
        # Aplica filtros locais
        if self.local_filters:
            # Mapeamento correto: nome da coluna -> índice no banco de dados
            column_to_db_index = {
                'Emissão CT-e': 1,
                'Numero CT-e': 0,
                'Notas': 2,
                'Remetente': 3,
                'Destinatário': 4,
                'Cidade Destino CT-e': 5,
                'Representante Entrega': 6,
                'Filial Resp. Entrega': 7,
                'Status Entrega Tela SAC': 8,
                'Data Chegada': 9,
                'Vendedor': 10,
                'Previsão Entrega': 11,
                'Última Ocorrência': 12
            }
            
            filtered_data = []
            for row in data:
                match = True
                for col_name, filter_config in self.local_filters.items():
                    db_col_index = column_to_db_index.get(col_name, 0)
                    
                    cell_value = str(row[db_col_index]).lower()
                    filter_value = filter_config['value'].lower()
                    filter_type = filter_config['type']
                    
                    if filter_type == 'contains':
                        if filter_value not in cell_value:
                            match = False
                            break
                    elif filter_type == 'not_contains':
                        if filter_value in cell_value:
                            match = False
                            break
                
                if match:
                    filtered_data.append(row)
            data = filtered_data
        
        # Aplica ordenação se houver colunas configuradas
        if self.sort_columns:
            data = self.sort_data(data)
        
        # Insere dados na tabela (reordena para corresponder às colunas)
        # Mapeia todas as colunas no formato de exibição
        all_columns_order = [1, 0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]  # emissao_cte, numero_cte, notas, ...
        visible_indices = self.get_visible_indices()
        
        for row in data:
            # Monta a linha completa
            full_row = [row[i] for i in all_columns_order]
            # Filtra apenas as colunas visíveis
            display_row = tuple(full_row[i] for i in visible_indices)
            
            # Obtém numero_cte (chave primária, está no índice 0 do row)
            numero_cte = str(row[0])
            
            # Aplica highlight se existir
            if numero_cte in self.highlights:
                color = self.highlights[numero_cte]
                self.tree.insert('', tk.END, values=display_row, tags=(color,))
            else:
                self.tree.insert('', tk.END, values=display_row)
        
        self.status_var.set(f"Total de registros: {len(data)}")
    
    def sort_data(self, data):
        """Ordena os dados com base nas colunas configuradas"""
        if not self.sort_columns:
            return data
        
        # Mapeamento de nomes de colunas para índices no banco de dados
        column_to_db_index = {
            self.normalize_column_name('Emissão CT-e'): 1,
            self.normalize_column_name('Numero CT-e'): 0,
            self.normalize_column_name('Notas'): 2,
            self.normalize_column_name('Remetente'): 3,
            self.normalize_column_name('Destinatário'): 4,
            self.normalize_column_name('Cidade Destino CT-e'): 5,
            self.normalize_column_name('Representante Entrega'): 6,
            self.normalize_column_name('Filial Resp. Entrega'): 7,
            self.normalize_column_name('Status Entrega Tela SAC'): 8,
            self.normalize_column_name('Data Chegada'): 9,
            self.normalize_column_name('Vendedor'): 10,
            self.normalize_column_name('Previsão Entrega'): 11,
            self.normalize_column_name('Última Ocorrência'): 12
        }
        
        # Cria lista de dados para ordenar
        data_list = list(data)
        
        # Ordena por múltiplas colunas (do último para o primeiro para manter prioridade)
        for column_name, direction in reversed(self.sort_columns):
            normalized_column_name = self.normalize_column_name(column_name)
            column_index = column_to_db_index.get(normalized_column_name, 0)
            reverse = (direction == 'desc')
            
            # Ordena com tratamento de valores vazios e numéricos
            data_list.sort(
                key=lambda row: self.get_sort_key(row[column_index], normalized_column_name),
                reverse=reverse
            )
        
        return data_list
    
    def normalize_column_name(self, column_name):
        """Normaliza nome de coluna para tornar o mapeamento mais tolerante a variações."""
        if column_name is None:
            return ''

        normalized = str(column_name).strip().strip('"\'')
        normalized = re.sub(r'\s+', ' ', normalized)
        return normalized.lower()

    def parse_date_value(self, value):
        """Converte texto de data para datetime aceitando formatos comuns do CSV."""
        if value is None:
            return None

        value_str = str(value).strip()
        if not value_str:
            return None

        # Normaliza variações comuns vindas de bancos/exports
        # Ex.: "28/12/2021", 2021-12-28T00:00:00, 28/12/2021 10:30:00.123
        value_str = value_str.strip('"\'')
        value_str = value_str.replace('T', ' ')
        value_str = re.sub(r'\.(\d+)$', '', value_str)  # remove milissegundos no final
        value_str = re.sub(r'([+-]\d{2}:?\d{2}|Z)$', '', value_str).strip()  # remove timezone

        date_formats = [
            '%d/%m/%Y',
            '%d/%m/%Y %H:%M',
            '%d/%m/%Y %H:%M:%S',
            '%d-%m-%Y',
            '%d-%m-%Y %H:%M',
            '%d-%m-%Y %H:%M:%S',
            '%Y-%m-%d',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%d %H:%M:%S'
        ]

        for date_format in date_formats:
            try:
                return datetime.strptime(value_str, date_format)
            except ValueError:
                continue

        return None

    def get_sort_key(self, value, column_name=None):
        """Retorna chave de ordenação com suporte a data, número e texto."""
        if not value or str(value).strip() == '':
            return (2, '')  # Valores vazios vão por último
        
        value_str = str(value).strip()
        normalized_column_name = self.normalize_column_name(column_name)

        # Ordenação por data para colunas específicas.
        date_columns = {
            self.normalize_column_name('Emissão CT-e'),
            self.normalize_column_name('Data Chegada'),
            self.normalize_column_name('Previsão Entrega')
        }
        if normalized_column_name in date_columns:
            parsed_date = self.parse_date_value(value_str)
            if parsed_date is not None:
                return (0, parsed_date)
            # Se não conseguir converter, mantém como texto para não quebrar ordenação.
            return (1, value_str.lower())
        
        # Tenta converter para número (para ordenação numérica correta)
        try:
            # Remove espaços e tenta converter para float
            num_value = float(value_str.replace(',', '.').replace(' ', ''))
            return (0, num_value)  # Números primeiro
        except ValueError:
            # Se não for número, retorna como string em lowercase
            return (1, value_str.lower())  # Strings depois
    
    def import_csv(self):
        """Importa arquivo CSV"""
        file_path = filedialog.askopenfilename(
            title="Selecione o arquivo CSV",
            filetypes=[("CSV files", "*.csv"), ("Todos os arquivos", "*.*")]
        )
        
        if not file_path:
            return
        
        try:
            imported_count = 0
            skipped_count = 0
            filtered_count = 0
            
            with open(file_path, 'r', encoding='utf-8-sig') as file:
                reader = csv.reader(file, delimiter=';')
                
                # Pula a primeira linha (título)
                next(reader, None)
                
                # Pula a segunda linha (cabeçalho - já sabemos as colunas)
                next(reader, None)
                
                for row in reader:
                    if len(row) < 13:
                        continue
                    
                    # Mapeia os dados
                    data_dict = {
                        'emissao_cte': row[0].strip(),
                        'numero_cte': row[1].strip(),
                        'notas': row[2].strip(),
                        'remetente': row[3].strip(),
                        'destinatario': row[4].strip(),
                        'cidade_destino_cte': row[5].strip(),
                        'representante_entrega': row[6].strip(),
                        'filial_resp_entrega': row[7].strip(),
                        'status_entrega_tela_sac': row[8].strip(),
                        'data_chegada': row[9].strip(),
                        'vendedor': row[10].strip(),
                        'previsao_entrega': row[11].strip(),
                        'ultima_ocorrencia': row[12].strip()
                    }
                    
                    # Aplica filtros globais
                    if self.global_filters:
                        match = True
                        for col_name, filter_value in self.global_filters.items():
                            # Mapeia nome da coluna para chave do dicionário
                            col_key = col_name.lower().replace(' ', '_').replace('-', '_').replace('.', '')
                            col_key = col_key.replace('ct_e', 'cte')
                            
                            if filter_value.lower() not in data_dict.get(col_key, '').lower():
                                match = False
                                break
                        
                        if not match:
                            filtered_count += 1
                            continue
                    
                    # Verifica se já existe
                    if self.db.cte_exists(data_dict['numero_cte']):
                        skipped_count += 1
                        continue
                    
                    # Insere no banco
                    if self.db.insert_row(data_dict):
                        imported_count += 1
            
            messagebox.showinfo(
                "Importação Concluída",
                f"Importados: {imported_count}\n"
                f"Ignorados (já existentes): {skipped_count}\n"
                f"Filtrados (filtro global): {filtered_count}"
            )
            
            self.load_data()
            
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao importar CSV:\n{str(e)}")
    
    def config_global_filter(self):
        """Configura filtros globais (aplicados na importação)"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Configurar Filtro Global")
        dialog.geometry("400x200")
        
        ttk.Label(dialog, text="Filtro Global (aplicado na importação)").pack(pady=10)
        
        frame = ttk.Frame(dialog, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(frame, text="Coluna:").grid(row=0, column=0, padx=5, pady=5)
        column_var = tk.StringVar()
        column_combo = ttk.Combobox(frame, textvariable=column_var, 
                                    values=self.column_labels, state="readonly", width=25)
        column_combo.grid(row=0, column=1, padx=5, pady=5)
        
        ttk.Label(frame, text="Valor contém:").grid(row=1, column=0, padx=5, pady=5)
        value_var = tk.StringVar()
        value_entry = ttk.Entry(frame, textvariable=value_var, width=27)
        value_entry.grid(row=1, column=1, padx=5, pady=5)
        
        def add_filter():
            col = column_var.get()
            val = value_var.get()
            if col and val:
                self.global_filters[col] = val
                self.save_config()
                update_filter_list()
        
        def remove_filter():
            col = column_var.get()
            if col in self.global_filters:
                del self.global_filters[col]
                self.save_config()
                update_filter_list()
        
        def clear_filters():
            self.global_filters.clear()
            self.save_config()
            update_filter_list()
        
        def update_filter_list():
            filter_text = "\n".join([f"{k}: {v}" for k, v in self.global_filters.items()])
            filter_label.config(text=filter_text if filter_text else "Nenhum filtro ativo")
        
        button_frame = ttk.Frame(frame)
        button_frame.grid(row=2, column=0, columnspan=2, pady=10)
        
        ttk.Button(button_frame, text="Adicionar", command=add_filter).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Remover", command=remove_filter).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Limpar Todos", command=clear_filters).pack(side=tk.LEFT, padx=5)
        
        filter_label = ttk.Label(frame, text="Filtros ativos aparecerão aqui", foreground="blue")
        filter_label.grid(row=3, column=0, columnspan=2, pady=10)
        
        update_filter_list()
    

    
    def clear_local_filters(self):
        """Limpa todos os filtros locais"""
        self.local_filters.clear()
        self.update_filter_indicators()
        self.load_data()
        messagebox.showinfo("Filtros Limpos", "Todos os filtros locais foram removidos")
    
    def perform_search(self):
        """Realiza busca ignorando filtros locais"""
        search_term = self.search_var.get().strip()
        if not search_term:
            self.load_data()
            self.clear_search_button.pack_forget()  # Oculta o botão X se busca estiver vazia
            return
        
        column = self.search_column_var.get()
        
        # Limpa a tabela
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Busca no banco (ignora filtros locais)
        results = self.db.search_data(column, search_term)
        
        # Insere resultados (apenas colunas visíveis)
        all_columns_order = [1, 0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        visible_indices = self.get_visible_indices()
        
        for row in results:
            # Monta a linha completa
            full_row = [row[i] for i in all_columns_order]
            # Filtra apenas as colunas visíveis
            display_row = tuple(full_row[i] for i in visible_indices)
            
            # Obtém numero_cte (está no índice 0 do row)
            numero_cte = str(row[0])
            
            # Aplica highlight se existir
            if numero_cte in self.highlights:
                color = self.highlights[numero_cte]
                self.tree.insert('', tk.END, values=display_row, tags=(color,))
            else:
                self.tree.insert('', tk.END, values=display_row)
        
        self.status_var.set(f"Resultados da busca: {len(results)}")
        
        # Mostra o botão X após realizar a busca
        self.clear_search_button.pack(side=tk.LEFT, padx=5)
    
    def clear_search(self):
        """Limpa a busca e retorna todas as linhas"""
        self.search_var.set('')  # Limpa o campo de busca
        self.clear_search_button.pack_forget()  # Oculta o botão X
        self.load_data()  # Recarrega dados normais com filtros locais
    
    def edit_column_labels(self):
        """Permite editar os labels das colunas e visibilidade"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Editar Labels e Visibilidade das Colunas")
        dialog.geometry("650x500")
        
        ttk.Label(dialog, text="Editar nomes e visibilidade das colunas:").pack(pady=10)
        
        canvas = tk.Canvas(dialog)
        scrollbar = ttk.Scrollbar(dialog, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        entry_vars = []
        checkbox_vars = []
        
        for i, (original, current) in enumerate(zip(self.original_columns, self.column_labels)):
            frame = ttk.Frame(scrollable_frame)
            frame.pack(fill=tk.X, padx=10, pady=5)
            
            # Checkbox para visibilidade
            check_var = tk.BooleanVar(value=self.visible_columns.get(original, True))
            checkbox = ttk.Checkbutton(frame, variable=check_var)
            checkbox.pack(side=tk.LEFT, padx=5)
            checkbox_vars.append((original, check_var))
            
            ttk.Label(frame, text=f"{original}:", width=22, anchor=tk.W).pack(side=tk.LEFT, padx=5)
            
            var = tk.StringVar(value=current)
            entry = ttk.Entry(frame, textvariable=var, width=30)
            entry.pack(side=tk.LEFT, padx=5)
            entry_vars.append(var)
        
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y, pady=10)
        
        def save_labels():
            new_labels = [var.get() for var in entry_vars]
            self.column_labels = new_labels
            
            # Atualiza visibilidade das colunas
            for original_col, check_var in checkbox_vars:
                self.visible_columns[original_col] = check_var.get()
            
            # Salva configuração
            self.save_config()
            
            # Recria a interface com as colunas visíveis
            self.recreate_tree()
            
            # Atualiza combo de busca
            if hasattr(self, 'search_options_frame'):
                for widget in self.search_options_frame.winfo_children():
                    if isinstance(widget, ttk.Combobox):
                        visible_labels = [self.column_labels[i] for i, col in enumerate(self.original_columns) 
                                         if self.visible_columns.get(col, True)]
                        widget['values'] = ["Todas"] + visible_labels
            
            dialog.destroy()
            messagebox.showinfo("Sucesso", "Labels e visibilidade atualizados!")
        
        ttk.Button(dialog, text="Salvar", command=save_labels).pack(pady=10)
    
    def on_double_click(self, event):
        """Permite editar células com duplo clique"""
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return
        
        column = self.tree.identify_column(event.x)
        row_id = self.tree.identify_row(event.y)
        
        if not row_id:
            return
        
        # Obtém índice da coluna visível
        visible_col_index = int(column.replace('#', '')) - 1
        
        # Mapeia para índice da coluna real
        visible_indices = self.get_visible_indices()
        if visible_col_index >= len(visible_indices):
            return
        actual_col_index = visible_indices[visible_col_index]
        
        # Obtém o label da coluna
        column_label = self.column_labels[actual_col_index]
        original_column = self.original_columns[actual_col_index]
        
        # Não permite editar a coluna Numero CT-e (chave primária)
        if original_column == 'Numero CT-e':
            messagebox.showwarning("Aviso", "Não é possível editar a chave primária (Numero CT-e)")
            return
        
        # Obtém valores atuais
        current_values = self.tree.item(row_id, 'values')
        current_value = current_values[visible_col_index]
        
        # Encontra o Numero CT-e nos valores visíveis
        # O Numero CT-e está na posição 1 da lista original
        numero_cte_index_in_visible = None
        for i, idx in enumerate(visible_indices):
            if self.original_columns[idx] == 'Numero CT-e':
                numero_cte_index_in_visible = i
                break
        
        if numero_cte_index_in_visible is None:
            messagebox.showerror("Erro", "Coluna 'Numero CT-e' deve estar visível para editar células")
            return
        
        numero_cte = current_values[numero_cte_index_in_visible]
        
        # Cria janela de edição
        edit_window = tk.Toplevel(self.root)
        edit_window.title("Editar Célula")
        edit_window.geometry("400x150")
        
        ttk.Label(edit_window, text=f"Editando: {column_label}").pack(pady=10)
        
        value_var = tk.StringVar(value=current_value)
        entry = ttk.Entry(edit_window, textvariable=value_var, width=50)
        entry.pack(pady=10, padx=20)
        entry.focus()
        entry.select_range(0, tk.END)
        
        def save_edit():
            new_value = value_var.get()
            if self.db.update_cell(numero_cte, original_column, new_value):
                # Atualiza a visualização
                new_values = list(current_values)
                new_values[visible_col_index] = new_value
                self.tree.item(row_id, values=new_values)
                edit_window.destroy()
                self.status_var.set("Célula atualizada com sucesso")
            else:
                messagebox.showerror("Erro", "Não foi possível atualizar a célula")
        
        button_frame = ttk.Frame(edit_window)
        button_frame.pack(pady=10)
        
        ttk.Button(button_frame, text="Salvar", command=save_edit).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Cancelar", command=edit_window.destroy).pack(side=tk.LEFT, padx=5)
        
        # Permite salvar com Enter
        entry.bind('<Return>', lambda e: save_edit())
    
    def highlight_row(self):
        """Destaca a linha selecionada com uma cor"""
        # Verifica se há uma linha selecionada
        selected_items = self.tree.selection()
        if not selected_items:
            messagebox.showwarning("Aviso", "Selecione uma linha para destacar")
            return
        
        row_id = selected_items[0]
        current_values = self.tree.item(row_id, 'values')
        
        # Encontra o índice do Numero CT-e nas colunas visíveis
        visible_indices = self.get_visible_indices()
        numero_cte_index_in_visible = None
        for i, idx in enumerate(visible_indices):
            if self.original_columns[idx] == 'Numero CT-e':
                numero_cte_index_in_visible = i
                break
        
        if numero_cte_index_in_visible is None:
            messagebox.showerror("Erro", "Coluna 'Numero CT-e' deve estar visível para destacar linhas")
            return
        
        numero_cte = str(current_values[numero_cte_index_in_visible])
        
        # Cria diálogo de seleção de cor
        dialog = tk.Toplevel(self.root)
        dialog.title("Destacar Linha")
        dialog.geometry("300x320")
        dialog.transient(self.root)
        dialog.grab_set()
        
        ttk.Label(dialog, text=f"Escolha a cor para o CT-e: {numero_cte}", 
                 font=('', 10, 'bold')).pack(pady=15)
        
        def apply_color(color_name):
            if color_name:
                self.highlights[numero_cte] = color_name
                self.tree.item(row_id, tags=(color_name,))
            else:
                # Remove o highlight
                if numero_cte in self.highlights:
                    del self.highlights[numero_cte]
                self.tree.item(row_id, tags=())
            
            self.save_config()
            self.status_var.set(f"Linha {'destacada' if color_name else 'sem destaque'}")
            dialog.destroy()
        
        # Botões de cores
        button_frame = ttk.Frame(dialog)
        button_frame.pack(pady=10)
        
        for color_name, color_value in self.highlight_colors.items():
            btn = tk.Button(button_frame, 
                          text=color_name, 
                          bg=color_value,
                          width=15,
                          height=2,
                          command=lambda c=color_name: apply_color(c))
            btn.pack(pady=5)
        
        # Botão para remover cor
        ttk.Button(button_frame, 
                  text="Remover Destaque", 
                  command=lambda: apply_color(None)).pack(pady=10)
    
    def generate_report(self):
        """Gera relatório em PDF com as linhas visíveis"""
        # Verifica se há dados para exportar
        if not self.tree.get_children():
            messagebox.showwarning("Aviso", "Não há dados para exportar")
            return
        
        # Cria diálogo de seleção de colunas
        dialog = tk.Toplevel(self.root)
        dialog.title("Gerar Relatório PDF")
        dialog.geometry("400x400")
        dialog.transient(self.root)
        dialog.grab_set()
        
        ttk.Label(dialog, text="Selecione as colunas para o relatório:", 
                 font=('', 10, 'bold')).pack(pady=10)
        
        # Frame com scroll para checkboxes
        canvas = tk.Canvas(dialog)
        scrollbar = ttk.Scrollbar(dialog, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Checkboxes para cada coluna visível
        visible_labels = self.get_visible_labels()
        checkbox_vars = []
        
        for col_label in visible_labels:
            var = tk.BooleanVar(value=True)
            checkbox = ttk.Checkbutton(scrollable_frame, text=col_label, variable=var)
            checkbox.pack(anchor=tk.W, padx=20, pady=5)
            checkbox_vars.append((col_label, var))
        
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y, pady=10)
        
        # Botões
        button_frame = ttk.Frame(canvas)
        button_frame.pack(pady=10,side=tk.BOTTOM)
        
        def select_all():
            for _, var in checkbox_vars:
                var.set(True)
        
        def deselect_all():
            for _, var in checkbox_vars:
                var.set(False)
        
        def generate():
            # Obtém colunas selecionadas
            selected_columns = [col for col, var in checkbox_vars if var.get()]
            
            if not selected_columns:
                messagebox.showwarning("Aviso", "Selecione pelo menos uma coluna")
                return
            
            dialog.destroy()
            self.create_pdf_report(selected_columns)
        
        ttk.Button(button_frame, text="Selecionar Todas", command=select_all).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Desmarcar Todas", command=deselect_all).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Gerar PDF", command=generate).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Cancelar", command=dialog.destroy).pack(side=tk.LEFT, padx=5)
    
    def create_pdf_report(self, selected_columns):
        """Cria o arquivo PDF com os dados filtrados e ordenados"""
        # Solicita local para salvar o arquivo
        file_path = filedialog.asksaveasfilename(
            title="Salvar Relatório",
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("Todos os arquivos", "*.*")],
            initialfile=f"relatorio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        )
        
        if not file_path:
            return
        
        try:
            # Cria o documento PDF em paisagem
            doc = SimpleDocTemplate(file_path, pagesize=landscape(A4))
            elements = []
            styles = getSampleStyleSheet()
            
            # Título do relatório
            title = Paragraph(f"<b>Relatório de CT-e - {datetime.now().strftime('%d/%m/%Y %H:%M')}</b>", 
                            styles['Title'])
            elements.append(title)
            elements.append(Spacer(1, 0.5*cm))
            
            # Informações sobre filtros ativos
            info_lines = []
            if self.local_filters:
                info_lines.append("<b>Filtros Ativos:</b>")
                for col, config in self.local_filters.items():
                    filter_type = "Contém" if config['type'] == 'contains' else "Não contém"
                    info_lines.append(f"  • {col}: {filter_type} '{config['value']}'")
            
            if self.sort_columns:
                if info_lines:
                    info_lines.append("")
                info_lines.append("<b>Ordenação:</b>")
                for idx, (col, direction) in enumerate(self.sort_columns, 1):
                    sort_dir = "Crescente" if direction == 'asc' else "Decrescente"
                    info_lines.append(f"  {idx}. {col}: {sort_dir}")
            
            if info_lines:
                info_text = "<br/>".join(info_lines)
                info_para = Paragraph(info_text, styles['Normal'])
                elements.append(info_para)
                elements.append(Spacer(1, 0.5*cm))
            
            # Obtém índices das colunas selecionadas
            visible_labels = self.get_visible_labels()
            selected_indices = [i for i, col in enumerate(visible_labels) if col in selected_columns]
            
            # Estilo para células com quebra de linha
            cell_style = ParagraphStyle(
                'CellStyle',
                parent=styles['Normal'],
                fontSize=7,
                leading=9,
                wordWrap='CJK'
            )
            
            header_style = ParagraphStyle(
                'HeaderStyle',
                parent=styles['Normal'],
                fontSize=8,
                fontName='Helvetica-Bold',
                leading=10,
                wordWrap='CJK',
                textColor=colors.whitesmoke
            )
            
            # Prepara dados da tabela
            # Cabeçalho com Paragraphs
            table_data = [[Paragraph(str(col), header_style) for col in selected_columns]]
            
            # Coleta dados das linhas visíveis com Paragraphs para quebra automática
            for item in self.tree.get_children():
                values = self.tree.item(item, 'values')
                row_data = [Paragraph(str(values[i]) if i < len(values) else '', cell_style) 
                           for i in selected_indices]
                table_data.append(row_data)
            
            # Calcula largura das colunas com base no número de colunas
            page_width = landscape(A4)[0] - 2*cm
            col_width = page_width / len(selected_columns)
            col_widths = [col_width] * len(selected_columns)
            
            # Cria tabela
            table = Table(table_data, colWidths=col_widths, repeatRows=1)
            
            # Estilo da tabela
            table_style = TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 8),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('TOPPADDING', (0, 0), (-1, 0), 8),
                ('LEFTPADDING', (0, 0), (-1, -1), 5),
                ('RIGHTPADDING', (0, 0), (-1, -1), 5),
                ('TOPPADDING', (0, 1), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 1), (-1, -1), 5),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 7),
            ])
            
            # Alterna cores das linhas
            for i in range(1, len(table_data)):
                if i % 2 == 0:
                    table_style.add('BACKGROUND', (0, i), (-1, i), colors.lightgrey)
            
            table.setStyle(table_style)
            elements.append(table)
            
            # Rodapé com total de registros
            elements.append(Spacer(1, 0.5*cm))
            footer = Paragraph(f"<b>Total de registros:</b> {len(table_data) - 1}", styles['Normal'])
            elements.append(footer)
            
            # Gera o PDF
            doc.build(elements)
            
            messagebox.showinfo("Sucesso", f"Relatório gerado com sucesso!\n\n{file_path}")
            self.status_var.set(f"Relatório salvo em: {file_path}")
            
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao gerar relatório:\n{str(e)}")
    
    def on_closing(self):
        """Fecha o banco de dados ao fechar a aplicação"""
        self.db.close()
        self.root.destroy()


def main():
    root = tk.Tk()
    app = SpreadsheetApp(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()


if __name__ == "__main__":
    main()
