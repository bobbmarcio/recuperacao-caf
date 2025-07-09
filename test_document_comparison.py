#!/usr/bin/env python3
"""
Testar sistema de comparação de documentos MongoDB
"""

from run_caf_analysis import documents_are_different

def test_document_comparison():
    """Testa a função de comparação de documentos"""
    
    print("🧪 Teste de Comparação de Documentos MongoDB")
    print("=" * 50)
    
    # Documento base
    doc1 = {
        '_id': 'different_id',
        'idUnidadeFamiliar': 'same-uuid',
        'possuiMaoObraContratada': False,
        'dataCriacao': '2025-01-01',
        'dataAtualizacao': '2025-01-01T10:00:00',
        'tipoTerreno': {'id': 1, 'descricao': 'Agricultura'},
        'enquadramentoRendas': [
            {'id': 'uuid1', 'tipoEnquadramentoRenda': {'id': 1, 'descricao': 'A'}}
        ]
    }
    
    # Documento idêntico (apenas _id diferente)
    doc2 = {
        '_id': 'another_different_id',
        'idUnidadeFamiliar': 'same-uuid',
        'possuiMaoObraContratada': False,
        'dataCriacao': '2025-01-01',
        'dataAtualizacao': '2025-01-02T15:30:00',  # Data diferente mas ignorada
        'tipoTerreno': {'id': 1, 'descricao': 'Agricultura'},
        'enquadramentoRendas': [
            {'id': 'uuid1', 'tipoEnquadramentoRenda': {'id': 1, 'descricao': 'A'}}
        ]
    }
    
    # Documento com diferença real
    doc3 = {
        '_id': 'yet_another_id',
        'idUnidadeFamiliar': 'same-uuid',
        'possuiMaoObraContratada': True,  # MUDOU!
        'dataCriacao': '2025-01-01',
        'dataAtualizacao': '2025-01-03T08:00:00',
        'tipoTerreno': {'id': 1, 'descricao': 'Agricultura'},
        'enquadramentoRendas': [
            {'id': 'uuid1', 'tipoEnquadramentoRenda': {'id': 1, 'descricao': 'A'}}
        ]
    }
    
    # Documento com enquadramento diferente
    doc4 = {
        '_id': 'fourth_id',
        'idUnidadeFamiliar': 'same-uuid',
        'possuiMaoObraContratada': False,
        'dataCriacao': '2025-01-01',
        'dataAtualizacao': '2025-01-04T12:00:00',
        'tipoTerreno': {'id': 1, 'descricao': 'Agricultura'},
        'enquadramentoRendas': [
            {'id': 'uuid1', 'tipoEnquadramentoRenda': {'id': 2, 'descricao': 'B'}}  # MUDOU!
        ]
    }
    
    # Testes
    print("📋 Teste 1: Documentos idênticos (exceto _id e dataAtualizacao)")
    result1 = documents_are_different(doc1, doc2)
    print(f"   Resultado: {'DIFERENTES' if result1 else 'IGUAIS'} ✅" if not result1 else f"   Resultado: {'DIFERENTES' if result1 else 'IGUAIS'} ❌")
    
    print("\n📋 Teste 2: Documento com campo boolean alterado")
    result2 = documents_are_different(doc1, doc3)
    print(f"   Resultado: {'DIFERENTES' if result2 else 'IGUAIS'} ✅" if result2 else f"   Resultado: {'DIFERENTES' if result2 else 'IGUAIS'} ❌")
    
    print("\n📋 Teste 3: Documento com enquadramento alterado")
    result3 = documents_are_different(doc1, doc4)
    print(f"   Resultado: {'DIFERENTES' if result3 else 'IGUAIS'} ✅" if result3 else f"   Resultado: {'DIFERENTES' if result3 else 'IGUAIS'} ❌")
    
    print("\n🎯 Resumo dos testes:")
    print(f"   - Ignorar _id e dataAtualizacao: {'✅ PASS' if not result1 else '❌ FAIL'}")
    print(f"   - Detectar mudança em campo: {'✅ PASS' if result2 else '❌ FAIL'}")
    print(f"   - Detectar mudança em array: {'✅ PASS' if result3 else '❌ FAIL'}")

if __name__ == "__main__":
    test_document_comparison()
