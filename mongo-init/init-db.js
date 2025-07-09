// Script de inicialização do MongoDB para o projeto de recuperação CAF
// Este script cria o banco de dados e usuários necessários

// Conectar ao banco de dados de auditoria
db = db.getSiblingDB('audit_db');

// Criar usuário para a aplicação
db.createUser({
  user: 'app_user',
  pwd: 'app_password',
  roles: [
    {
      role: 'readWrite',
      db: 'audit_db'
    }
  ]
});

// Criar coleções com validação
db.createCollection('data_changes', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['table_name', 'primary_key_value', 'column_name', 'change_timestamp'],
      properties: {
        table_name: {
          bsonType: 'string',
          description: 'Nome da tabela onde ocorreu a alteração'
        },
        primary_key_value: {
          bsonType: 'string',
          description: 'Valor da chave primária do registro alterado'
        },
        column_name: {
          bsonType: 'string',
          description: 'Nome da coluna que foi alterada'
        },
        old_value: {
          description: 'Valor anterior da coluna'
        },
        new_value: {
          description: 'Novo valor da coluna'
        },
        change_timestamp: {
          bsonType: 'date',
          description: 'Timestamp da alteração'
        },
        dump_source: {
          bsonType: 'string',
          description: 'Nome do dump de origem'
        },
        dump_target: {
          bsonType: 'string',
          description: 'Nome do dump de destino'
        },
        audit_metadata: {
          bsonType: 'object',
          properties: {
            inserted_at: {
              bsonType: 'date'
            },
            data_type: {
              bsonType: 'string'
            },
            change_type: {
              bsonType: 'string',
              enum: ['insert', 'update', 'delete']
            }
          }
        }
      }
    }
  }
});

db.createCollection('dump_metadata', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['file_path', 'file_name', 'processed_at'],
      properties: {
        file_path: {
          bsonType: 'string',
          description: 'Caminho completo do arquivo de dump'
        },
        file_name: {
          bsonType: 'string',
          description: 'Nome do arquivo de dump'
        },
        tables_found: {
          bsonType: 'array',
          items: {
            bsonType: 'string'
          },
          description: 'Lista de tabelas encontradas no dump'
        },
        total_records: {
          bsonType: 'number',
          description: 'Total de registros processados'
        },
        processed_at: {
          bsonType: 'date',
          description: 'Timestamp do processamento'
        },
        status: {
          bsonType: 'string',
          enum: ['processed', 'error', 'processing'],
          description: 'Status do processamento'
        }
      }
    }
  }
});

// Criar índices para otimizar consultas
db.data_changes.createIndex({ table_name: 1 });
db.data_changes.createIndex({ column_name: 1 });
db.data_changes.createIndex({ change_timestamp: -1 });
db.data_changes.createIndex({ table_name: 1, column_name: 1 });
db.data_changes.createIndex({ table_name: 1, primary_key_value: 1 });
db.data_changes.createIndex({ dump_source: 1, dump_target: 1 });

db.dump_metadata.createIndex({ file_name: 1 });
db.dump_metadata.createIndex({ processed_at: -1 });
db.dump_metadata.createIndex({ status: 1 });

print('✅ Banco de dados audit_db inicializado com sucesso!');
print('📊 Coleções criadas: data_changes, dump_metadata');
print('🔍 Índices criados para otimização de consultas');
print('👤 Usuário app_user criado com permissões readWrite');
