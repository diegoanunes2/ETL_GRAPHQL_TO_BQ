## =================================== Query Exemplo ====================================================##

# Query GraphQl
query_exemplo = """
        query {
	exemplo(page: $p, limit:80, createdOrModifiedStart: "$start",createdOrModifiedEnd: "$end") {
		list {
			exemplo_id:id
			campo_1
			campo_2 {
				informacao_1
				informacao_2
			}
		}
		pageInfo {
			hasNext
			next
			total
			totalPages
		}
	}
}
"""

# =========================== Querys Incrementais =================

## ======================= Incremental ===========================##
## ========================  exemplo  ============================##

exemplo_incremental = '''
MERGE `banco_destino` AS destino
USING (
  SELECT * FROM `banco_origem`) AS origem
ON destino.exemplo_id = origem.exemplo_id
WHEN MATCHED THEN
  UPDATE SET
destino.exemplo_id = origem.exemplo_id,
destino.campo_1 = origem.campo_1,
destino.campo_2_informacao_1 = origem.campo_2_informacao_1, 
destino.campo_2_informacao_2 = origem.campo_2_informacao_2 
WHEN NOT MATCHED THEN
  INSERT (
exemplo_id,
campo_1,
campo_2_informacao_1,
campo_2_informacao_2
)
VALUES (
origem.exemplo_id,
origem.campo_1,
origem.campo_2_informacao_1, 
origem.campo_2_informacao_2 

)
'''

query_remocao = '''
DELETE FROM `tabela.final` AS tf
WHERE NOT EXISTS (
  SELECT 1
  FROM `tabela.temporaria` AS tt
  WHERE tf.notas_id = tt.notas_id
)
'''
