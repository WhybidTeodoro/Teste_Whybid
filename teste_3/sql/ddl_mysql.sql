CREATE TABLE operadoras (
    registro_ans    INT PRIMARY KEY,
    cnpj            VARCHAR(14) NOT NULL,
    razao_social    TEXT NOT NULL,
    modalidade      TEXT NULL,
    uf              CHAR(2) NULL,

    UNIQUE KEY uq_operadoras_cnpj (cnpj),
    KEY idx_operadoras_uf (uf),
    KEY idx_operadoras_razao (razao_social(150))
) ENGINE=InnoDB;

-- Trade-off tipos:
-- - dinheiro: DECIMAL(18,2) para precisão
-- - ano/trimestre: SMALLINT (mais simples que DATE aqui)
CREATE TABLE despesas_consolidadas (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    registro_ans    INT NOT NULL,
    ano             SMALLINT NOT NULL,
    trimestre       SMALLINT NOT NULL,
    valor_despesas  DECIMAL(18,2) NOT NULL,

    CONSTRAINT fk_despesas_operadora
        FOREIGN KEY (registro_ans) REFERENCES operadoras (registro_ans),

    CONSTRAINT ck_trimestre CHECK (trimestre BETWEEN 1 AND 4),
    CONSTRAINT ck_valor_positivo CHECK (valor_despesas > 0),

    KEY idx_despesas_registro_periodo (registro_ans, ano, trimestre),
    KEY idx_despesas_periodo (ano, trimestre)
) ENGINE=InnoDB;

-- Observação: este CSV é agregado por RazaoSocial + UF
CREATE TABLE despesas_agregadas (
    id                          BIGINT AUTO_INCREMENT PRIMARY KEY,
    razao_social                TEXT NOT NULL,
    uf                          CHAR(2) NOT NULL,
    total_despesas              DECIMAL(18,2) NOT NULL,
    media_despesas_trimestre    DECIMAL(18,2) NOT NULL,
    desvio_padrao_despesas      DECIMAL(18,2) NOT NULL,
    qtd_trimestres              SMALLINT NOT NULL,

    CONSTRAINT ck_total_nao_negativo CHECK (total_despesas >= 0),

    KEY idx_aggr_uf_total (uf, total_despesas)
) ENGINE=InnoDB;

-- =========================
-- STAGING TABLES (tudo como texto)
-- =========================

CREATE TABLE stg_operadoras (
    registro_operadora  TEXT,
    cnpj                TEXT,
    razao_social        TEXT,
    modalidade          TEXT,
    uf                  TEXT
) ENGINE=InnoDB;

CREATE TABLE stg_despesas_consolidadas (
    reg_ans         TEXT,
    cnpj            TEXT,
    razaosocial     TEXT,
    ano             TEXT,
    trimestre       TEXT,
    valordespesas   TEXT
) ENGINE=InnoDB;

CREATE TABLE stg_despesas_agregadas (
    razaosocial                 TEXT,
    uf                          TEXT,
    totaldespesas               TEXT,
    mediadespesastrimestre      TEXT,
    desviopadraodespesas        TEXT,
    qtdtrimestres               TEXT
) ENGINE=InnoDB;