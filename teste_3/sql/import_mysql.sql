-- =========================================================
-- 1) Limpa staging antes de importar
-- =========================================================
TRUNCATE stg_operadoras;
TRUNCATE stg_despesas_consolidadas;
TRUNCATE stg_despesas_agregadas;

LOAD DATA LOCAL INFILE 'C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\Relatorio_cadop.csv'
INTO TABLE stg_operadoras
CHARACTER SET latin1
FIELDS TERMINATED BY ';'
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@registro, @cnpj, @razao, @nome_fantasia, @modalidade, @logradouro, @numero,
 @complemento, @bairro, @cidade, @uf, @cep, @ddd, @telefone, @fax,
 @email, @representante, @cargo, @regiao, @data_registro)
SET
registro_operadora = @registro,
cnpj = @cnpj,
razao_social = @razao,
modalidade = @modalidade,
uf = @uf;

SET GLOBAL local_infile = 'ON';
LOAD DATA INFILE 'CAMINHO_ABSOLUTO/despesas_eventos_sinistros.csv'
INTO TABLE stg_despesas_consolidadas
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ';'
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(reg_ans, cnpj, razaosocial, ano, trimestre, valordespesas);


LOAD DATA INFILE 'CAMINHO_ABSOLUTO/despesas_agregadas.csv'
INTO TABLE stg_despesas_agregadas
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ';'
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(razaosocial, uf, totaldespesas, mediadespesastrimestre, desviopadraodespesas, qtdtrimestres);


INSERT INTO operadoras (registro_ans, cnpj, razao_social, modalidade, uf)
SELECT
    CAST(TRIM(registro_operadora) AS UNSIGNED) AS registro_ans,
    REGEXP_REPLACE(IFNULL(cnpj, ''), '[^0-9]', '') AS cnpj_limpo,
    COALESCE(NULLIF(TRIM(razao_social), ''), 'DESCONHECIDO') AS razao_social,
    NULLIF(TRIM(modalidade), '') AS modalidade,
    CASE
        WHEN LENGTH(TRIM(uf)) >= 2 THEN UPPER(LEFT(TRIM(uf), 2))
        ELSE NULL
    END AS uf
FROM stg_operadoras
WHERE TRIM(registro_operadora) REGEXP '^[0-9]+$'
ON DUPLICATE KEY UPDATE
    cnpj = VALUES(cnpj),
    razao_social = VALUES(razao_social),
    modalidade = VALUES(modalidade),
    uf = VALUES(uf);


INSERT INTO despesas_consolidadas (registro_ans, ano, trimestre, valor_despesas)
SELECT
    CAST(TRIM(s.reg_ans) AS UNSIGNED) AS registro_ans,
    CAST(TRIM(s.ano) AS UNSIGNED) AS ano,
    CAST(TRIM(s.trimestre) AS UNSIGNED) AS trimestre,
    CAST(REPLACE(TRIM(s.valordespesas), ',', '.') AS DECIMAL(18,2)) AS valor_despesas
FROM stg_despesas_consolidadas s
JOIN operadoras o
  ON o.registro_ans = CAST(TRIM(s.reg_ans) AS UNSIGNED)
WHERE
    TRIM(s.reg_ans) REGEXP '^[0-9]+$'
    AND TRIM(s.ano) REGEXP '^[0-9]{4}$'
    AND TRIM(s.trimestre) REGEXP '^[1-4]$'
    AND TRIM(s.valordespesas) <> ''
    AND REPLACE(TRIM(s.valordespesas), ',', '.') REGEXP '^[0-9]+(\\.[0-9]+)?$'
    AND CAST(REPLACE(TRIM(s.valordespesas), ',', '.') AS DECIMAL(18,2)) > 0;


INSERT INTO despesas_agregadas (
    razao_social, uf, total_despesas, media_despesas_trimestre, desvio_padrao_despesas, qtd_trimestres
)
SELECT
    COALESCE(NULLIF(TRIM(razaosocial), ''), 'DESCONHECIDO') AS razao_social,
    UPPER(LEFT(TRIM(uf), 2)) AS uf,
    CAST(REPLACE(TRIM(totaldespesas), ',', '.') AS DECIMAL(18,2)) AS total_despesas,
    CAST(REPLACE(TRIM(mediadespesastrimestre), ',', '.') AS DECIMAL(18,2)) AS media_despesas_trimestre,
    CAST(REPLACE(TRIM(desviopadraodespesas), ',', '.') AS DECIMAL(18,2)) AS desvio_padrao_despesas,
    CAST(TRIM(qtdtrimestres) AS UNSIGNED) AS qtd_trimestres
FROM stg_despesas_agregadas
WHERE
    TRIM(uf) <> ''
    AND REPLACE(TRIM(totaldespesas), ',', '.') REGEXP '^[0-9]+(\\.[0-9]+)?$';
