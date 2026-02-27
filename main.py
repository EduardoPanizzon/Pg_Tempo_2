import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import sqlite3
import csv
from datetime import datetime
import os
import json


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
        
        # Inicializa banco de dados
        self.db = DatabaseManager()
        
        # Filtros
        self.global_filters = {}  # Formato: {coluna: valor}
        self.local_filters = {}   # Formato: {coluna: {"type": "contains"|"not_contains", "value": valor}}
        self.config_file = 'filtros_globais.json'
        
        # Carrega filtros globais salvos
        self.load_global_filters()
        
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
        ttk.Button(top_frame, text="Editar Labels", command=self.edit_column_labels).pack(side=tk.LEFT, padx=5)
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
        column_combo = ttk.Combobox(self.search_options_frame, 
                                    textvariable=self.search_column_var,
                                    values=["Todas"] + self.column_labels,
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
        
        # Treeview (tabela)
        self.tree = ttk.Treeview(table_frame, 
                                columns=self.column_labels,
                                show='headings',
                                yscrollcommand=vsb.set,
                                xscrollcommand=hsb.set)
        
        vsb.config(command=self.tree.yview)
        hsb.config(command=self.tree.xview)
        
        # Configura colunas
        for col in self.column_labels:
            self.tree.heading(col, text=col, command=lambda c=col: self.on_column_click(c))
            self.tree.column(col, width=120, anchor=tk.W)
        
        self.tree.pack(fill=tk.BOTH, expand=True)
        
        # Bind para edição de células
        self.tree.bind('<Double-1>', self.on_double_click)
        
        # Atualiza cabeçalhos com indicadores de filtro
        self.update_column_headers()
        
        # Status bar
        self.status_var = tk.StringVar()
        self.status_var.set("Pronto")
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
    
    def load_global_filters(self):
        """Carrega filtros globais salvos do arquivo"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    self.global_filters = json.load(f)
        except Exception as e:
            print(f"Erro ao carregar filtros globais: {e}")
            self.global_filters = {}
    
    def save_global_filters(self):
        """Salva filtros globais em arquivo"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.global_filters, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Erro ao salvar filtros globais: {e}")
    
    def update_column_headers(self):
        """Atualiza cabeçalhos das colunas com indicador de filtro ativo (*)"""
        for i, col in enumerate(self.column_labels):
            if col in self.local_filters:
                display_text = f"{col} *"
            else:
                display_text = col
            self.tree.heading(i, text=display_text, command=lambda c=col: self.on_column_click(c))
    
    def on_column_click(self, column_name):
        """Abre diálogo de filtro ao clicar no cabeçalho da coluna"""
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
                self.update_column_headers()
                self.load_data()
                dialog.destroy()
        
        def remove_filter():
            if column_name in self.local_filters:
                del self.local_filters[column_name]
                self.update_column_headers()
                self.load_data()
            dialog.destroy()
        
        ttk.Button(button_frame, text="Aplicar Filtro", command=apply_filter).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Remover Filtro", command=remove_filter).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Cancelar", command=dialog.destroy).pack(side=tk.LEFT, padx=5)
        
        # Enter para aplicar
        value_entry.bind('<Return>', lambda e: apply_filter())
    
    def load_data(self):
        """Carrega dados do banco e aplica filtros locais"""
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
        
        # Insere dados na tabela (reordena para corresponder às colunas)
        for row in data:
            # row[0] é numero_cte (chave primária)
            # Reordena: emissao_cte, numero_cte, notas, ...
            display_row = (row[1], row[0], row[2], row[3], row[4], row[5], 
                          row[6], row[7], row[8], row[9], row[10], row[11], row[12])
            self.tree.insert('', tk.END, values=display_row)
        
        self.status_var.set(f"Total de registros: {len(data)}")
    
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
                self.save_global_filters()
                update_filter_list()
        
        def remove_filter():
            col = column_var.get()
            if col in self.global_filters:
                del self.global_filters[col]
                self.save_global_filters()
                update_filter_list()
        
        def clear_filters():
            self.global_filters.clear()
            self.save_global_filters()
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
        self.update_column_headers()
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
        
        # Insere resultados
        for row in results:
            display_row = (row[1], row[0], row[2], row[3], row[4], row[5], 
                          row[6], row[7], row[8], row[9], row[10], row[11], row[12])
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
        """Permite editar os labels das colunas"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Editar Labels das Colunas")
        dialog.geometry("500x500")
        
        ttk.Label(dialog, text="Editar nomes das colunas:").pack(pady=10)
        
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
        for i, (original, current) in enumerate(zip(self.original_columns, self.column_labels)):
            frame = ttk.Frame(scrollable_frame)
            frame.pack(fill=tk.X, padx=10, pady=5)
            
            ttk.Label(frame, text=f"{original}:", width=25, anchor=tk.W).pack(side=tk.LEFT, padx=5)
            
            var = tk.StringVar(value=current)
            entry = ttk.Entry(frame, textvariable=var, width=30)
            entry.pack(side=tk.LEFT, padx=5)
            entry_vars.append(var)
        
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y, pady=10)
        
        def save_labels():
            new_labels = [var.get() for var in entry_vars]
            self.column_labels = new_labels
            
            # Atualiza cabeçalhos da tabela
            self.update_column_headers()
            
            # Atualiza combo de busca
            if hasattr(self, 'search_options_frame'):
                for widget in self.search_options_frame.winfo_children():
                    if isinstance(widget, ttk.Combobox):
                        widget['values'] = ["Todas"] + self.column_labels
            
            dialog.destroy()
            messagebox.showinfo("Sucesso", "Labels atualizados!")
        
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
        
        # Obtém índice da coluna
        col_index = int(column.replace('#', '')) - 1
        
        # Não permite editar a coluna Numero CT-e (chave primária)
        if col_index == 1:
            messagebox.showwarning("Aviso", "Não é possível editar a chave primária (Numero CT-e)")
            return
        
        # Obtém valores atuais
        current_values = self.tree.item(row_id, 'values')
        current_value = current_values[col_index]
        numero_cte = current_values[1]  # Numero CT-e está na posição 1
        
        # Cria janela de edição
        edit_window = tk.Toplevel(self.root)
        edit_window.title("Editar Célula")
        edit_window.geometry("400x150")
        
        ttk.Label(edit_window, text=f"Editando: {self.column_labels[col_index]}").pack(pady=10)
        
        value_var = tk.StringVar(value=current_value)
        entry = ttk.Entry(edit_window, textvariable=value_var, width=50)
        entry.pack(pady=10, padx=20)
        entry.focus()
        entry.select_range(0, tk.END)
        
        def save_edit():
            new_value = value_var.get()
            if self.db.update_cell(numero_cte, self.column_labels[col_index], new_value):
                # Atualiza a visualização
                new_values = list(current_values)
                new_values[col_index] = new_value
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
