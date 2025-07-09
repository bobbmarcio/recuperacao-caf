# Estrutura MongoDB para Unidade Familiar - CAF

Este documento descreve a estrutura de dados MongoDB que deve ser utilizada para armazenar as alterações detectadas na análise incremental de dumps do sistema CAF.

## Estrutura Base

Baseado no exemplo fornecido, cada documento de auditoria de uma unidade familiar deve seguir a seguinte estrutura:

```json
{
  "_id": ObjectId("auto-gerado"),
  "_versao": 1,
  "idUnidadeFamiliar": "8ab52a30-89c1-436b-b006-62a9d48153cb",
  "possuiMaoObraContratada": false,
  "dataValidade": "2028-01-28",
  "descricaoInativacao": null,
  "dataCriacao": "2025-01-28",
  "dataAtualizacao": {
    "$date": "2025-01-28T18:03:40.627Z"
  },
  "dataAtivacao": {
    "$date": "2025-01-28T18:03:40.627Z"
  },
  "dataPrimeiraAtivacao": "2025-01-28",
  "dataBloqueio": null,
  "dataInativacao": null,
  "migradaCaf2": false,
  "possuiVersaoCaf3": true,
  "migradaIncra": false,
  "tipoTerreno": {
    "id": 1,
    "descricao": "Agricultura, Pecuária e Outras atividades"
  },
  "caracterizacaoArea": {
    "id": 1,
    "descricao": "Assentamento da Reforma Agrária"
  },
  "tipoSituacao": {
    "id": 1,
    "descricao": "ATIVA"
  },
  "caf": {
    "id": "ee6dbe80-169f-4a44-bac2-5511485d38af",
    "numeroCaf": 344,
    "uf": "CE",
    "dataCriacao": "2025-01-28",
    "tipoCaf": {
      "id": 1,
      "descricao": "Unidade Familiar"
    },
    "entidadeEmissora": {
      "id": "d54d65a9-a9a0-49a2-ac3f-af138aaa4663",
      "cnpj": "05371711000196",
      "razaoSocial": "EMP DE ASSIST TEC E EXT RURAL DO EST DO CE EMATERCE",
      "dataCriacao": {
        "$date": "2025-01-24T15:40:38.215Z"
      },
      "dataInativacao": null,
      "motivoInativacao": null
    }
  },
  "entidadeEmissora": null,
  "enderecoPessoa": {
    "id": "456bfcb7-5ba7-4d44-b00d-3da907f85695",
    "endereco": {
      "id": "1d8f90f5-41ce-4315-9499-3bb630ffba90",
      "uf": "PE",
      "cep": "55038565",
      "logradouro": "Rua 1",
      "complemento": "Complemento 1",
      "numero": 1,
      "referencia": "Bairro 1",
      "dataCriacao": {
        "$date": "2025-01-28T21:02:07.031Z"
      },
      "dataAtualizacao": {
        "$date": "2025-01-28T21:02:07.031Z"
      },
      "codigoMunicipio": {
        "codigoMunicipio": "2604106",
        "codigoUf": "26",
        "siglaUf": "PE",
        "nome": "Caruaru"
      }
    }
  },
  "enquadramentoRendas": [
    {
      "id": "a1994ef3-46c3-49c5-97b0-5cf417445122",
      "tipoEnquadramentoRenda": {
        "id": 1,
        "descricao": "A"
      }
    },
    {
      "id": "80bdbe66-1ac7-4e7f-94cb-8a0d1f18b36c",
      "tipoEnquadramentoRenda": {
        "id": 2,
        "descricao": "AC"
      }
    },
    {
      "id": "cc16eb71-3e7f-43e6-9dbd-122500b05fc2",
      "tipoEnquadramentoRenda": {
        "id": 4,
        "descricao": "V"
      }
    }
  ],
  "numeroCaf": "CE012025.01.000000344CAF"
}
```

## Mapeamento de Campos SQL -> MongoDB

| Campo SQL                               | Campo MongoDB              | Tipo no MongoDB           |
|----------------------------------------|----------------------------|---------------------------|
| `id_unidade_familiar`                  | `idUnidadeFamiliar`        | string (UUID)             |
| `st_possui_mao_obra`                   | `possuiMaoObraContratada`  | boolean                   |
| `dt_validade`                          | `dataValidade`             | string (YYYY-MM-DD)       |
| `ds_inativacao`                        | `descricaoInativacao`      | string \| null            |
| `dt_criacao`                           | `dataCriacao`              | string (YYYY-MM-DD)       |
| `dt_atualizacao`                       | `dataAtualizacao`          | {$date: ISO_STRING}       |
| `dt_ativacao`                          | `dataAtivacao`             | {$date: ISO_STRING}       |
| `dt_primeira_ativacao`                 | `dataPrimeiraAtivacao`     | string (YYYY-MM-DD)       |
| `dt_bloqueio`                          | `dataBloqueio`             | string \| null            |
| `dt_inativacao`                        | `dataInativacao`           | string \| null            |
| `st_migrada_caf_2`                     | `migradaCaf2`              | boolean                   |
| `st_possui_versao_caf3`                | `possuiVersaoCaf3`         | boolean                   |
| `st_migrada_incra`                     | `migradaIncra`             | boolean                   |
| `id_tipo_terreno_ufpr`                 | `tipoTerreno`              | {id: number, descricao}   |
| `id_caracterizacao_area`               | `caracterizacaoArea`       | {id: number, descricao}   |
| `id_tipo_situacao_unidade_familiar`    | `tipoSituacao`             | {id: number, descricao}   |

## Regras de Conversão

### 1. Campos de Data
- **Timestamp completo** (`dt_atualizacao`, `dt_ativacao`): usar `{$date: "ISO_STRING"}`
- **Data simples** (`dt_criacao`, `dt_validade`): usar string `"YYYY-MM-DD"`

### 2. Campos Booleanos
- Converter diretamente para `true`/`false`
- Campos: `st_possui_mao_obra`, `st_migrada_caf_2`, `st_possui_versao_caf3`, `st_migrada_incra`

### 3. Campos de Referência (IDs)
- Expandir para objetos com `id` e `descricao`
- Requer lookup nas tabelas de referência correspondentes
- Exemplo: `id_tipo_terreno_ufpr` → `{"id": 1, "descricao": "Agricultura, Pecuária e Outras atividades"}`

### 4. Campos Nulos
- Manter como `null` (não string `"null"`)

### 5. Relacionamentos 1:N
- Usar arrays com objetos aninhados
- Exemplo: `enquadramentoRendas` é um array de objetos

## Estrutura de Auditoria

Além dos campos da unidade familiar, incluir metadados de auditoria:

```json
{
  // ... campos da unidade familiar ...
  "audit_metadata": {
    "change_type": "update",
    "changed_field": "possuiMaoObraContratada",
    "old_value": true,
    "new_value": false,
    "change_timestamp": "2025-06-26T10:30:00Z",
    "dump_source": "caf_20250301",
    "dump_target": "caf_20250401",
    "inserted_at": "2025-06-26T15:45:30Z"
  }
}
```

## Implementação no Código

### Arquivo: `src/mongo_inserter.py`

O método `_convert_unidade_familiar_change()` implementa essa conversão:

1. **Mapeamento de campos**: `_get_field_mapping()`
2. **Conversão de valores**: `_convert_field_value()`
3. **Estrutura de auditoria**: metadados sobre a alteração

### Configuração: `config/monitoring_config.yaml`

Define quais campos da tabela `S_UNIDADE_FAMILIAR` devem ser monitorados:

```yaml
"S_UNIDADE_FAMILIAR":
  primary_key: id_unidade_familiar
  columns:
    - st_possui_mao_obra
    - dt_validade
    - dt_atualizacao
    # ... outros campos
```

## Expansão Futura

Para uma implementação completa, seria necessário:

1. **Lookup automático** para expandir IDs em objetos completos
2. **Agregação de dados** de tabelas relacionadas (CAF, endereço, etc.)
3. **Versionamento** para histórico completo de alterações
4. **Índices MongoDB** para consultas eficientes

## Exemplo de Uso

```python
# Para testar a estrutura:
python demo_mongodb_structure.py

# Para executar análise incremental completa:
python run_caf_analysis.py
```

## Consultas MongoDB Úteis

```javascript
// Buscar alterações em uma unidade familiar específica
db.caf_audit.find({"idUnidadeFamiliar": "8ab52a30-89c1-436b-b006-62a9d48153cb"})

// Buscar alterações por tipo
db.caf_audit.find({"audit_metadata.change_type": "update"})

// Buscar alterações em campo específico
db.caf_audit.find({"audit_metadata.changed_field": "possuiMaoObraContratada"})

// Buscar por período
db.caf_audit.find({
  "audit_metadata.change_timestamp": {
    "$gte": ISODate("2025-01-01"),
    "$lte": ISODate("2025-12-31")
  }
})
```

---

Este documento serve como referência para a estrutura de dados MongoDB esperada pelo sistema de análise incremental de dumps CAF.
