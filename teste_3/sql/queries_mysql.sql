WITH base AS (
    SELECT
        d.registro_ans,
        o.razao_social,
        (d.ano * 10 + d.trimestre) AS periodo,
        SUM(d.valor_despesas) AS total_periodo
    FROM despesas_consolidadas d
    JOIN operadoras o ON o.registro_ans = d.registro_ans
    GROUP BY d.registro_ans, o.razao_social, (d.ano * 10 + d.trimestre)
),
limits AS (
    SELECT
        registro_ans,
        MIN(periodo) AS first_periodo,
        MAX(periodo) AS last_periodo
    FROM base
    GROUP BY registro_ans
),
paired AS (
    SELECT
        b1.registro_ans,
        b1.razao_social,
        b1.total_periodo AS first_total,
        b2.total_periodo AS last_total
    FROM limits l
    JOIN base b1
      ON b1.registro_ans = l.registro_ans
     AND b1.periodo = l.first_periodo
    JOIN base b2
      ON b2.registro_ans = l.registro_ans
     AND b2.periodo = l.last_periodo
)
SELECT
    registro_ans,
    razao_social,
    first_total,
    last_total,
    ROUND(((last_total - first_total) / NULLIF(first_total, 0)) * 100, 2) AS crescimento_percentual
FROM paired
WHERE first_total > 0
ORDER BY crescimento_percentual DESC
LIMIT 5;




WITH por_operadora_uf AS (
    SELECT
        o.uf,
        d.registro_ans,
        SUM(d.valor_despesas) AS total_operadora_uf
    FROM despesas_consolidadas d
    JOIN operadoras o ON o.registro_ans = d.registro_ans
    WHERE o.uf IS NOT NULL AND o.uf <> ''
    GROUP BY o.uf, d.registro_ans
),
por_uf AS (
    SELECT
        uf,
        SUM(total_operadora_uf) AS total_uf,
        AVG(total_operadora_uf) AS media_por_operadora
    FROM por_operadora_uf
    GROUP BY uf
)
SELECT
    uf,
    ROUND(total_uf, 2) AS total_despesas_uf,
    ROUND(media_por_operadora, 2) AS media_despesas_por_operadora
FROM por_uf
ORDER BY total_uf DESC
LIMIT 5;




WITH por_operadora_trimestre AS (
    SELECT
        registro_ans,
        ano,
        trimestre,
        SUM(valor_despesas) AS total_operadora_trimestre
    FROM despesas_consolidadas
    GROUP BY registro_ans, ano, trimestre
),
media_geral_trimestre AS (
    SELECT
        ano,
        trimestre,
        AVG(total_operadora_trimestre) AS media_trimestre
    FROM por_operadora_trimestre
    GROUP BY ano, trimestre
),
acima_media AS (
    SELECT
        p.registro_ans,
        COUNT(*) AS qtd_trimestres_acima
    FROM por_operadora_trimestre p
    JOIN media_geral_trimestre m
      ON m.ano = p.ano
     AND m.trimestre = p.trimestre
    WHERE p.total_operadora_trimestre > m.media_trimestre
    GROUP BY p.registro_ans
)
SELECT
    COUNT(*) AS operadoras_acima_media_em_2_trimestres
FROM acima_media
WHERE qtd_trimestres_acima >= 2;