"""
Integrador para inserção de dados de auditoria no MongoDB
"""

import pymongo
from pymongo import MongoClient
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import asdict
from loguru import logger

from data_comparator import DataChange
from config import DatabaseConfig


class MongoAuditInserter:
    """Insere dados de auditoria no MongoDB"""
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.client: Optional[MongoClient] = None
        self.database = None
        self.collection = None
        
    def connect(self) -> bool:
        """
        Estabelece conexão com o MongoDB
        
        Returns:
            True se conectou com sucesso, False caso contrário
        """
        try:
            logger.info(f"Conectando ao MongoDB: {self.db_config.mongodb_database}")
            
            self.client = MongoClient(
                self.db_config.mongodb_connection_string,
                serverSelectionTimeoutMS=5000
            )
            
            # Testar conexão
            self.client.admin.command('ping')
            
            self.database = self.client[self.db_config.mongodb_database]
            self.collection = self.database[self.db_config.mongodb_collection]
            
            logger.info("Conexão com MongoDB estabelecida com sucesso")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao conectar com MongoDB: {e}")
            return False
    
    def disconnect(self) -> None:
        """Fecha a conexão com o MongoDB"""
        if self.client:
            self.client.close()
            logger.info("Conexão com MongoDB fechada")
    
    def insert_changes(self, changes: List[DataChange]) -> bool:
        """
        Insere lista de alterações no MongoDB
        
        Args:
            changes: Lista de alterações a inserir
            
        Returns:
            True se inseriu com sucesso, False caso contrário
        """
        if not changes:
            logger.info("Nenhuma alteração para inserir")
            return True
        
        if self.collection is None:
            logger.error("Conexão com MongoDB não estabelecida")
            return False
        
        try:
            logger.info(f"Inserindo {len(changes)} alterações no MongoDB")
            
            # Converter alterações para documentos MongoDB
            documents = []
            for change in changes:
                doc = self._change_to_document(change)
                documents.append(doc)
            
            # Inserir em lote
            result = self.collection.insert_many(documents)
            
            logger.info(f"Inseridas {len(result.inserted_ids)} alterações no MongoDB")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao inserir alterações no MongoDB: {e}")
            return False
    
    def _change_to_document(self, change: DataChange) -> Dict[str, Any]:
        """
        Converte uma alteração para documento MongoDB
        
        Args:
            change: Alteração a converter
            
        Returns:
            Documento MongoDB
        """
        # Se for uma alteração na tabela S_UNIDADE_FAMILIAR, usar estrutura específica
        if change.table_name == 'S_UNIDADE_FAMILIAR':
            return self._convert_unidade_familiar_change(change)
        
        # Para outras tabelas, usar estrutura genérica
        doc = {
            'table_name': change.table_name,
            'primary_key_value': str(change.primary_key_value),
            'column_name': change.column_name,
            'old_value': change.old_value,
            'new_value': change.new_value,
            'change_timestamp': change.timestamp if isinstance(change.timestamp, datetime) else datetime.fromisoformat(str(change.timestamp)),
            'dump_source': change.dump_source,
            'dump_target': change.dump_target,
            'audit_metadata': {
                'inserted_at': datetime.now(),
                'data_type': type(change.old_value).__name__ if change.old_value is not None else 'None',
                'change_type': self._determine_change_type(change.old_value, change.new_value)
            }
        }
        
        return doc

    def _convert_unidade_familiar_change(self, change: DataChange) -> Dict[str, Any]:
        """
        Converte alteração da unidade familiar para estrutura MongoDB específica
        
        Args:
            change: Alteração da unidade familiar
            
        Returns:
            Documento MongoDB com estrutura específica para unidade familiar
        """
        doc = {
            '_versao': 1,
            'idUnidadeFamiliar': str(change.primary_key_value),
            'audit_metadata': {
                'change_type': self._determine_change_type(change.old_value, change.new_value),
                'changed_field': self._map_field_name(change.column_name),
                'old_value': change.old_value,
                'new_value': change.new_value,
                'change_timestamp': change.timestamp if isinstance(change.timestamp, datetime) else datetime.fromisoformat(str(change.timestamp)),
                'dump_source': change.dump_source,
                'dump_target': change.dump_target,
                'inserted_at': datetime.now()
            }
        }
        
        # Mapear campos específicos conforme a estrutura MongoDB esperada
        field_mapping = self._get_field_mapping()
        mapped_field = field_mapping.get(change.column_name, change.column_name)
        
        # Adicionar o valor atual do campo mapeado
        if change.new_value is not None:
            doc[mapped_field] = self._convert_field_value(change.column_name, change.new_value)
        
        return doc

    def _map_field_name(self, sql_field: str) -> str:
        """
        Mapeia nomes de campos SQL para nomes MongoDB
        
        Args:
            sql_field: Nome do campo SQL
            
        Returns:
            Nome do campo MongoDB correspondente
        """
        field_mapping = self._get_field_mapping()
        return field_mapping.get(sql_field, sql_field)

    def _get_field_mapping(self) -> Dict[str, str]:
        """
        Retorna mapeamento de campos SQL para MongoDB
        
        Returns:
            Dicionário com mapeamento de campos
        """
        return {
            'id_unidade_familiar': 'idUnidadeFamiliar',
            'st_possui_mao_obra': 'possuiMaoObraContratada',
            'dt_validade': 'dataValidade',
            'ds_inativacao': 'descricaoInativacao',
            'dt_criacao': 'dataCriacao',
            'dt_atualizacao': 'dataAtualizacao',
            'dt_ativacao': 'dataAtivacao',
            'dt_primeira_ativacao': 'dataPrimeiraAtivacao',
            'dt_bloqueio': 'dataBloqueio',
            'dt_inativacao': 'dataInativacao',
            'st_migrada_caf_2': 'migradaCaf2',
            'st_possui_versao_caf3': 'possuiVersaoCaf3',
            'st_migrada_incra': 'migradaIncra',
            'id_tipo_terreno_ufpr': 'tipoTerreno',
            'id_caracterizacao_area': 'caracterizacaoArea',
            'id_tipo_situacao_unidade_familiar': 'tipoSituacao',
            'id_entidade_emissora': 'entidadeEmissora',
            'id_motivo_inativacao': 'motivoInativacao',
            'id_publico_tradicional': 'publicoTradicional'
        }

    def _convert_field_value(self, sql_field: str, value: Any) -> Any:
        """
        Converte valor do campo SQL para formato MongoDB
        
        Args:
            sql_field: Nome do campo SQL
            value: Valor a converter
            
        Returns:
            Valor convertido para MongoDB
        """
        # Campos de data para formato ISO ou $date
        date_fields = [
            'dt_validade', 'dt_criacao', 'dt_atualizacao', 'dt_ativacao',
            'dt_primeira_ativacao', 'dt_bloqueio', 'dt_inativacao'
        ]
        
        if sql_field in date_fields and value is not None:
            if isinstance(value, str):
                try:
                    # Tentar converter string para datetime
                    if 'T' in value:  # ISO format
                        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    else:  # Date only
                        dt = datetime.strptime(value, '%Y-%m-%d')
                    
                    # Para campos com timestamp, retornar objeto $date
                    if sql_field in ['dt_atualizacao', 'dt_ativacao']:
                        return {'$date': dt.isoformat() + 'Z'}
                    else:
                        return dt.strftime('%Y-%m-%d')
                except:
                    return value
            elif hasattr(value, 'isoformat'):
                # Para campos com timestamp, retornar objeto $date
                if sql_field in ['dt_atualizacao', 'dt_ativacao']:
                    return {'$date': value.isoformat() + 'Z'}
                else:
                    return value.strftime('%Y-%m-%d')
        
        # Campos booleanos
        boolean_fields = ['st_possui_mao_obra', 'st_migrada_caf_2', 'st_possui_versao_caf3', 'st_migrada_incra']
        if sql_field in boolean_fields and value is not None:
            return bool(value)
        
        # Campos de ID que devem ser objetos com id e descricao
        # Para simplificar, por enquanto apenas retornar o ID
        # Em uma implementação completa, deveria fazer lookup nas tabelas de referência
        id_fields = [
            'id_tipo_terreno_ufpr', 'id_caracterizacao_area', 
            'id_tipo_situacao_unidade_familiar', 'id_entidade_emissora'
        ]
        if sql_field in id_fields and value is not None:
            return {'id': value, 'descricao': None}  # Placeholder - deveria fazer lookup
        
        return value
    
    def insert_dump_metadata(self, dump_info: Dict[str, Any]) -> bool:
        """
        Insere metadados do dump processado
        
        Args:
            dump_info: Informações do dump
            
        Returns:
            True se inseriu com sucesso, False caso contrário
        """
        if self.database is None:
            logger.error("Conexão com MongoDB não estabelecida")
            return False
        
        try:
            metadata_collection = self.database['dump_metadata']
            
            document = {
                **dump_info,
                'processed_at': datetime.now(),
                'status': 'processed'
            }
            
            metadata_collection.insert_one(document)
            logger.info(f"Metadados do dump inseridos: {dump_info.get('file_path', 'unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao inserir metadados do dump: {e}")
            return False
    
    def get_last_processed_dump(self) -> Optional[Dict[str, Any]]:
        """
        Retorna informações do último dump processado
        
        Returns:
            Dicionário com informações do último dump ou None
        """
        if self.database is None:
            return None
        
        try:
            metadata_collection = self.database['dump_metadata']
            
            last_dump = metadata_collection.find_one(
                {'status': 'processed'},
                sort=[('processed_at', pymongo.DESCENDING)]
            )
            
            return last_dump
            
        except Exception as e:
            logger.error(f"Erro ao buscar último dump processado: {e}")
            return None
    
    def query_changes(
        self,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Consulta alterações no MongoDB com filtros
        
        Args:
            table_name: Filtrar por nome da tabela
            column_name: Filtrar por nome da coluna
            start_date: Data inicial
            end_date: Data final
            limit: Limite de resultados
            
        Returns:
            Lista de alterações
        """
        if self.collection is None:
            logger.error("Conexão com MongoDB não estabelecida")
            return []
        
        try:
            # Construir filtro
            filter_query = {}
            
            if table_name:
                filter_query['table_name'] = table_name
            
            if column_name:
                filter_query['column_name'] = column_name
            
            if start_date or end_date:
                date_filter = {}
                if start_date:
                    date_filter['$gte'] = start_date
                if end_date:
                    date_filter['$lte'] = end_date
                filter_query['change_timestamp'] = date_filter
            
            # Executar consulta
            cursor = self.collection.find(filter_query).limit(limit).sort('change_timestamp', -1)
            changes = list(cursor)
            
            logger.info(f"Encontradas {len(changes)} alterações com os filtros aplicados")
            return changes
            
        except Exception as e:
            logger.error(f"Erro ao consultar alterações: {e}")
            return []
    
    def create_indexes(self) -> bool:
        """
        Cria índices para otimizar consultas
        
        Returns:
            True se criou índices com sucesso, False caso contrário
        """
        if self.collection is None:
            logger.error("Conexão com MongoDB não estabelecida")
            return False
        
        try:
            logger.info("Criando índices no MongoDB")
            
            # Índices para consultas comuns
            indexes = [
                [('table_name', 1)],
                [('column_name', 1)],
                [('change_timestamp', -1)],
                [('table_name', 1), ('column_name', 1)],
                [('table_name', 1), ('primary_key_value', 1)],
                [('dump_source', 1), ('dump_target', 1)]
            ]
            
            for index in indexes:
                self.collection.create_index(index)
            
            logger.info("Índices criados com sucesso")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao criar índices: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatísticas da coleção de auditoria
        
        Returns:
            Dicionário com estatísticas
        """
        if self.collection is None:
            return {}
        
        try:
            # Estatísticas básicas
            total_changes = self.collection.count_documents({})
            
            # Alterações por tabela
            pipeline_tables = [
                {'$group': {'_id': '$table_name', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}}
            ]
            tables_stats = list(self.collection.aggregate(pipeline_tables))
            
            # Alterações por coluna
            pipeline_columns = [
                {'$group': {'_id': {'table': '$table_name', 'column': '$column_name'}, 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}},
                {'$limit': 10}
            ]
            columns_stats = list(self.collection.aggregate(pipeline_columns))
            
            # Período de dados
            oldest = self.collection.find_one(sort=[('change_timestamp', 1)])
            newest = self.collection.find_one(sort=[('change_timestamp', -1)])
            
            stats = {
                'total_changes': total_changes,
                'tables_with_changes': len(tables_stats),
                'changes_by_table': tables_stats,
                'top_changed_columns': columns_stats,
                'date_range': {
                    'oldest': oldest['change_timestamp'] if oldest else None,
                    'newest': newest['change_timestamp'] if newest else None
                }
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas: {e}")
            return {}
