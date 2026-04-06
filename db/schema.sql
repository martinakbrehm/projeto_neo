-- Schema SQL para projeto de banco profissional
-- Charset e engine para MySQL

CREATE DATABASE IF NOT EXISTS pipeline_banco CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
USE pipeline_banco;

-- Distribuidoras
CREATE TABLE IF NOT EXISTS distribuidoras (
  id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
  nome VARCHAR(50) NOT NULL UNIQUE,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabela para retornos/processamentos da Macro API (registros separados da `tabela_macros`)
CREATE TABLE IF NOT EXISTS tabela_macro_api (
  id INT NOT NULL AUTO_INCREMENT,
  cliente_id INT NOT NULL,
  cliente_uc_id INT DEFAULT NULL,
  distribuidora_id TINYINT UNSIGNED DEFAULT NULL,
  resposta_id TINYINT UNSIGNED DEFAULT NULL,
  faturas_vencidas INT DEFAULT NULL,
  data_vencimento_fatura DATE DEFAULT NULL,
  parcelamento VARCHAR(100) DEFAULT NULL,
  faturas_negativadas INT DEFAULT NULL,
  total_a_vencer DECIMAL(12,2) DEFAULT NULL,
  data_contrato DATE DEFAULT NULL,
  categoria_tarifa VARCHAR(100) DEFAULT NULL,
  b_optante TINYINT(1) DEFAULT NULL,
  codigo TINYINT UNSIGNED DEFAULT NULL,
  -- http_status and retorno_codigo removed (not required)
  data_update DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  data_extracao DATETIME DEFAULT NULL,
  data_criacao DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  status ENUM('pendente','processando','reprocessar','consolidado','excluido') NOT NULL DEFAULT 'pendente',
  processed TINYINT(1) NOT NULL DEFAULT 0,  -- mantido para compatibilidade; prefira controlar via status='processando'
  PRIMARY KEY (id),
  KEY idx_macro_api_cliente (cliente_id)
  /* Foreign keys are added later in the file after referenced tables exist */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- View para automação da Macro API: por CPF+UC prioriza `pendente` (mais recente) senão `reprocessar` (mais recente)
CREATE OR REPLACE VIEW view_macro_api_automacao AS
SELECT tma.id,
       tma.cliente_id,
       tma.cliente_uc_id,
       tma.distribuidora_id,
       tma.resposta_id,
  -- api-specific fields
  tma.faturas_vencidas,
  tma.data_vencimento_fatura,
  tma.parcelamento,
  tma.faturas_negativadas,
  tma.total_a_vencer,
  tma.data_contrato,
  tma.categoria_tarifa,
  tma.b_optante,
  tma.codigo,
  tma.data_extracao,
  tma.data_update,
       tma.data_criacao,
       tma.status,
       tma.processed
FROM (
  SELECT
    tma2.*,
    c.cpf AS __cpf,
    cu.uc AS __uc,
    ROW_NUMBER() OVER (
      PARTITION BY c.cpf, COALESCE(cu.uc, '')
      ORDER BY (tma2.status = 'pendente') DESC, tma2.data_update DESC, tma2.id DESC
    ) AS rn
  FROM tabela_macro_api tma2
  JOIN clientes c ON tma2.cliente_id = c.id
  LEFT JOIN cliente_uc cu ON tma2.cliente_uc_id = cu.id
  WHERE tma2.status IN ('pendente','reprocessar')
    -- status='processando' é excluído automaticamente pelo filtro acima
) tma
WHERE tma.rn = 1;

-- Procedimento para retornar lotes da view (padrão 2000)
DELIMITER $$
CREATE PROCEDURE get_macro_api_batch(IN batch_size INT)
BEGIN
  IF batch_size IS NULL OR batch_size <= 0 THEN
    SET batch_size = 2000;
  END IF;
  SELECT * FROM view_macro_api_automacao ORDER BY data_update DESC, id DESC LIMIT batch_size;
END$$
DELIMITER ;

-- Procedure to link a `tabela_macro_api` record to a `cliente_uc`.
-- If the `cliente_uc` does not exist for the given client+uc, it will be created.
DELIMITER $$
CREATE PROCEDURE proc_macro_api_link_uc(
  IN p_macro_api_id INT,
  IN p_uc VARCHAR(50),
  IN p_distribuidora_id TINYINT UNSIGNED
)
BEGIN
  DECLARE v_cliente_id INT;
  DECLARE v_cliente_uc_id INT;
  DECLARE v_uc CHAR(10);

  -- Find cliente_id from the macro_api record
  SELECT cliente_id INTO v_cliente_id
  FROM tabela_macro_api
  WHERE id = p_macro_api_id
  LIMIT 1;

  IF v_cliente_id IS NULL THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'tabela_macro_api id not found';
  END IF;

  -- Normalize UC to 10 chars, left-padded with zeros
  SET v_uc = LPAD(REPLACE(p_uc, ' ', ''), 10, '0');

  -- Try to find existing cliente_uc for this client and uc
  SELECT id INTO v_cliente_uc_id
  FROM cliente_uc
  WHERE cliente_id = v_cliente_id
    AND uc = v_uc
  LIMIT 1;

  -- If not found, create it
  IF v_cliente_uc_id IS NULL THEN
    INSERT INTO cliente_uc (cliente_id, uc, distribuidora_id, data_criacao)
    VALUES (v_cliente_id, v_uc, p_distribuidora_id, CURRENT_TIMESTAMP);
    SET v_cliente_uc_id = LAST_INSERT_ID();
  END IF;

  -- Update tabela_macro_api to reference the cliente_uc and set distribuidora/resposta defaults when missing
  UPDATE tabela_macro_api
  SET cliente_uc_id = v_cliente_uc_id,
      distribuidora_id = IFNULL(distribuidora_id, p_distribuidora_id),
      resposta_id = IFNULL(resposta_id, (SELECT id FROM respostas WHERE status = 'pendente' ORDER BY id LIMIT 1))
  WHERE id = p_macro_api_id;

  -- Return the linked cliente_uc id
  SELECT v_cliente_uc_id AS cliente_uc_id;
END$$
DELIMITER ;

-- Índices recomendados para `tabela_macro_api`
ALTER TABLE tabela_macro_api
  ADD INDEX idx_macro_api_status_data (status, data_update, cliente_id, distribuidora_id),
  ADD INDEX idx_macro_api_particao (cliente_id, cliente_uc_id, status, data_update, id),
  ADD INDEX idx_macro_api_distrib_status (distribuidora_id, status, data_update);

-- Indexes to further support high-volume queries and joins
ALTER TABLE tabela_macro_api
  ADD INDEX idx_macro_api_cliente_status_update (cliente_id, status, data_update),
  ADD INDEX idx_macro_api_clienteuc_status_update (cliente_uc_id, status, data_update),
  ADD INDEX idx_macro_api_resposta (resposta_id),
  ADD INDEX idx_macro_api_codigo (codigo),
  ADD INDEX idx_macro_api_data_extracao (data_extracao),
  ADD INDEX idx_macro_api_processed_status (processed, status, data_update);

-- Respostas, Status e Mensagens de Erro (tabelas de apoio)
CREATE TABLE IF NOT EXISTS respostas (
  id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
  mensagem TEXT,
  status VARCHAR(50) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Adicionar um registro na tabela `respostas` para o status 'pendente' com ID 6
INSERT INTO respostas (id, mensagem, status) VALUES (6, 'Aguardando processamento', 'pendente')
ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status);

-- Adicionar respostas adicionais usadas pela Macro API
INSERT INTO respostas (id, mensagem, status) VALUES (7, 'Doc. Fiscal nao cadastrado no SAP', 'excluir')
ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status);

INSERT INTO respostas (id, mensagem, status) VALUES (8, 'Parceiro informado não possui conta contrato', 'excluir')
ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status);

INSERT INTO respostas (id, mensagem, status) VALUES (9, 'Status instalacao: desligado', 'reprocessar')
ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status);

INSERT INTO respostas (id, mensagem, status) VALUES (10, 'Status instalacao: ligado', 'consolidado')
ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status);

INSERT INTO respostas (id, mensagem, status) VALUES (11, 'ERRO', 'pendente')
ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status);

-- Tabela de clientes (informações imutáveis)
CREATE TABLE IF NOT EXISTS clientes (
  id INT NOT NULL AUTO_INCREMENT,
  cpf CHAR(11) NOT NULL,
  nome VARCHAR(255) DEFAULT NULL,
  data_nascimento DATE DEFAULT NULL,
  data_criacao DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  data_update DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY ux_clientes_cpf (cpf)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabela de enderecos (um cliente pode ter vários endereços no histórico)
CREATE TABLE IF NOT EXISTS enderecos (
  id INT NOT NULL AUTO_INCREMENT,
  cliente_id INT DEFAULT NULL,
  distribuidora_id TINYINT UNSIGNED DEFAULT NULL,
  endereco VARCHAR(255) DEFAULT NULL,
  numero VARCHAR(50) DEFAULT NULL,
  complemento VARCHAR(255) DEFAULT NULL,
  bairro VARCHAR(100) DEFAULT NULL,
  cidade VARCHAR(100) DEFAULT NULL,
  uf CHAR(2) DEFAULT NULL,
  cep VARCHAR(20) DEFAULT NULL,
  data_criacao DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_enderecos_cliente (cliente_id),
  CONSTRAINT fk_enderecos_cliente FOREIGN KEY (cliente_id) REFERENCES clientes (id) ON DELETE CASCADE,
  CONSTRAINT fk_enderecos_distribuidora FOREIGN KEY (distribuidora_id) REFERENCES distribuidoras (id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabela de telefones (um cliente pode ter vários telefones)
-- Armazenamos como BIGINT UNSIGNED para garantir valor numérico; use formato sem formatação (somente dígitos).
CREATE TABLE IF NOT EXISTS telefones (
  id INT NOT NULL AUTO_INCREMENT,
  cliente_id INT NOT NULL,
  telefone BIGINT UNSIGNED DEFAULT NULL,
  tipo VARCHAR(30) DEFAULT NULL,
  data_criacao DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_telefones_cliente (cliente_id),
  CONSTRAINT fk_telefones_cliente FOREIGN KEY (cliente_id) REFERENCES clientes (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabela equivalente a tabela_retornos_2: tabela_macros
-- Registra os retornos/processamentos por cliente + distribuidora
CREATE TABLE IF NOT EXISTS tabela_macros (
  id INT NOT NULL AUTO_INCREMENT,
  cliente_id INT NOT NULL,
  distribuidora_id TINYINT UNSIGNED NOT NULL,
  resposta_id TINYINT UNSIGNED DEFAULT NULL,
  qtd_faturas INT DEFAULT NULL,
  valor_debito DECIMAL(10,2) DEFAULT NULL,
  valor_credito DECIMAL(10,2) DEFAULT NULL,
  data_update DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  data_extracao DATETIME DEFAULT NULL,
  data_criacao DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  status ENUM('pendente','processando','reprocessar','consolidado','excluido') NOT NULL DEFAULT 'pendente',
  extraido TINYINT(1) NOT NULL DEFAULT 0,
  data_inic_parc DATE DEFAULT NULL,
  qtd_parcelas INT DEFAULT NULL,
  valor_parcelas DECIMAL(10,2) DEFAULT NULL,
  PRIMARY KEY (id),
  CONSTRAINT fk_tabela_macros_cliente FOREIGN KEY (cliente_id) REFERENCES clientes (id) ON DELETE CASCADE,
  CONSTRAINT fk_tabela_macros_distribuidora FOREIGN KEY (distribuidora_id) REFERENCES distribuidoras (id) ON DELETE RESTRICT,
  CONSTRAINT fk_tabela_macros_resposta FOREIGN KEY (resposta_id) REFERENCES respostas (id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Observação: as alterações de renomeação de colunas e adição de `data_update` foram
-- removidas pois as tabelas já são criadas abaixo com os nomes e colunas esperados
-- (evita falhas ao reaplicar o schema quando estruturas já existem).

-- Alterar a coluna `resposta_id` para ter valor padrão 6
ALTER TABLE tabela_macros
MODIFY COLUMN resposta_id TINYINT UNSIGNED DEFAULT 6;

-- Ajustar a coluna `data_update` para refletir a data de inserção e não ser atualizada automaticamente
ALTER TABLE tabela_macros
MODIFY COLUMN data_update DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP;

-- Tabela para o estado atual por (cliente_id, distribuidora_id)
-- Remover a tabela `tabela_macros_finalizados`
DROP TABLE IF EXISTS tabela_macros_finalizados;

-- Restaurar a lógica original da view `view_macros_finalizados`
CREATE OR REPLACE VIEW view_macros_finalizados AS
SELECT *
FROM tabela_macros
WHERE status = 'consolidado';

-- View unificada de consolidados (inclui origem da macro: 'macro' ou 'api')
-- Permite extrair todos os consolidados e identificar de qual macro veio o registro
CREATE OR REPLACE VIEW view_consolidados_unificado AS
SELECT
  'macro' AS origem,
  tm.id AS origem_id,
  tm.cliente_id,
  cu.id AS cliente_uc_id,
  tm.distribuidora_id,
  tm.resposta_id,
  tm.qtd_faturas,
  tm.valor_debito,
  tm.valor_credito,
  tm.data_update AS data_evento,
  tm.data_criacao,
  tm.status,
  -- api-specific fields (NULL para origem macro)
  NULL AS faturas_vencidas,
  NULL AS data_vencimento_fatura,
  NULL AS parcelamento,
  NULL AS faturas_negativadas,
  NULL AS total_a_vencer,
  NULL AS data_contrato,
  NULL AS categoria_tarifa,
  NULL AS b_optante,
  NULL AS codigo,
  NULL AS data_extracao
FROM tabela_macros tm
-- LEFT JOIN resolve cliente_uc por (cliente_id, distribuidora_id); mais eficiente que subquery correlacionada
LEFT JOIN cliente_uc cu ON cu.cliente_id = tm.cliente_id AND cu.distribuidora_id = tm.distribuidora_id
WHERE tm.status = 'consolidado'
UNION ALL
SELECT
  'api' AS origem,
  tma.id AS origem_id,
  tma.cliente_id,
  tma.cliente_uc_id,
  tma.distribuidora_id,
  tma.resposta_id,
  NULL AS qtd_faturas,
  NULL AS valor_debito,
  NULL AS valor_credito,
  tma.data_update AS data_evento,
  tma.data_criacao,
  tma.status,
  -- api-specific fields
  tma.faturas_vencidas,
  tma.data_vencimento_fatura,
  tma.parcelamento,
  tma.faturas_negativadas,
  tma.total_a_vencer,
  tma.data_contrato,
  tma.categoria_tarifa,
  tma.b_optante,
  tma.codigo,
  tma.data_extracao
FROM tabela_macro_api tma
WHERE tma.status = 'consolidado';

-- (removed) `view_macros_para_automacao` replaced by a single consolidated view `view_macros_automacao` below


-- Trigger para manter tabela_macros_finalizados sincronizada após inserts em tabela_macros
-- Remover o trigger `trg_after_insert_tabela_macros` pois a tabela associada não existe mais
DROP TRIGGER IF EXISTS trg_after_insert_tabela_macros;
-- Trigger removed: `trg_after_insert_tabela_macros` referenced non-existent table `tabela_macros_finalizados`.

-- Atualizar registros pendentes com uma resposta padrão
UPDATE tabela_macros
SET resposta_id = (SELECT id FROM respostas WHERE status = 'pendente')
WHERE status = 'pendente';

-- Tags (opcional)
-- CREATE TABLE IF NOT EXISTS tags (
--   id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
--   distribuidora_id TINYINT UNSIGNED NOT NULL,
--   nome VARCHAR(50) NOT NULL UNIQUE,
--   PRIMARY KEY (id),
--   CONSTRAINT fk_tags_distrib FOREIGN KEY (distribuidora_id) REFERENCES distribuidoras (id) ON DELETE CASCADE
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Índices e otimizações adicionais
-- idx_clientes_cpf removido: coberto pelo UNIQUE KEY ux_clientes_cpf já declarado na tabela
ALTER TABLE telefones ADD INDEX idx_telefones_numero (telefone);

-- Índices adicionais recomendados para performance das views/joins
-- (os índices de `cliente_uc` são adicionados após a criação da tabela mais abaixo)

ALTER TABLE tabela_macros
  ADD INDEX idx_tabela_macros_status_data_cliente_distrib (status, data_update, cliente_id, distribuidora_id),
  ADD INDEX idx_tabela_macros_cliente_distrib_data (cliente_id, distribuidora_id, data_update),
  ADD INDEX idx_tabela_macros_extraido_status_data (extraido, status, data_update),
  ADD INDEX idx_tabela_macros_distrib_data_evento (distribuidora_id, data_update);

-- Additional indexes to support heavy-read workloads on tabela_macros
ALTER TABLE tabela_macros
  ADD INDEX idx_tabela_macros_status_cliente_update (status, cliente_id, data_update),
  ADD INDEX idx_tabela_macros_resposta (resposta_id),
  ADD INDEX idx_tabela_macros_data_extracao (data_extracao);

-- Fim do schema

-- Tabelas de staging (simplificadas)
CREATE TABLE IF NOT EXISTS staging_imports (
  id INT NOT NULL AUTO_INCREMENT,
  filename VARCHAR(255) NOT NULL,
  distribuidora_nome VARCHAR(100) DEFAULT NULL,
  -- identifica para qual tabela/macrofiltro os dados devem ser processados
  target_macro_table VARCHAR(100) DEFAULT NULL,
  total_rows INT DEFAULT 0,
  rows_success INT DEFAULT 0,
  rows_failed INT DEFAULT 0,
  status ENUM('pending','processing','completed','failed') NOT NULL DEFAULT 'pending',
  imported_by VARCHAR(100) DEFAULT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  started_at DATETIME DEFAULT NULL,
  finished_at DATETIME DEFAULT NULL,
  PRIMARY KEY (id),
  INDEX idx_staging_status (status),
  INDEX idx_staging_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS staging_import_rows (
  id INT NOT NULL AUTO_INCREMENT,
  staging_id INT NOT NULL,
  row_idx INT DEFAULT NULL,
  raw_cpf VARCHAR(50) DEFAULT NULL,
  raw_nome VARCHAR(255) DEFAULT NULL,
  raw_telefone VARCHAR(255) DEFAULT NULL,
  raw_endereco VARCHAR(255) DEFAULT NULL,
  normalized_cpf CHAR(11) DEFAULT NULL,
  validation_status ENUM('new','valid','invalid','skipped') DEFAULT 'new',
  validation_message VARCHAR(255) DEFAULT NULL,
  processed_at DATETIME DEFAULT NULL,
  PRIMARY KEY (id),
  INDEX idx_staging_rows_staging (staging_id),
  INDEX idx_staging_rows_normcpf (normalized_cpf),
  CONSTRAINT fk_staging_rows_imports FOREIGN KEY (staging_id) REFERENCES staging_imports (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Atualizar a view para priorizar registros com status 'pendente' e incluir 'reprocessar' apenas se não houver 'pendente'
-- View that for each CPF+UC returns the most relevant record:
-- prefer `pendente` (most recent), otherwise the most recent `reprocessar`.
-- Uses window functions for performance and stable tie-break by `data_update` then `id`.
CREATE OR REPLACE VIEW view_macros_automacao AS
SELECT
  vm.id,
  vm.cliente_id,
  vm.distribuidora_id,
  vm.resposta_id,
  vm.qtd_faturas,
  vm.valor_debito,
  vm.valor_credito,
  vm.data_update,
  vm.data_extracao,
  vm.data_criacao,
  vm.status,
  vm.extraido,
  vm.data_inic_parc,
  vm.qtd_parcelas,
  vm.valor_parcelas
FROM (
  SELECT
    tm.*,
    c.cpf AS __cpf,
    cu.uc AS __uc,
    ROW_NUMBER() OVER (
      PARTITION BY c.cpf, COALESCE(cu.uc, '')
      ORDER BY
        (tm.status = 'pendente') DESC,
        tm.data_update DESC,
        tm.id DESC
    ) AS rn
  FROM tabela_macros tm
  JOIN clientes c ON tm.cliente_id = c.id
  -- LEFT JOIN garante que registros sem cliente_uc ainda apareçam na view
  -- (ex.: carregados antes da UC ser criada). Registros sem UC ficam com __uc = NULL.
  LEFT JOIN cliente_uc cu ON cu.cliente_id = tm.cliente_id AND cu.distribuidora_id = tm.distribuidora_id
  WHERE tm.status IN ('pendente','reprocessar')
) vm
WHERE vm.rn = 1;

-- Stored procedure to return the next batch of N records (default 2000)
DELIMITER $$
CREATE PROCEDURE get_macros_automacao_batch(IN batch_size INT)
BEGIN
  IF batch_size IS NULL OR batch_size <= 0 THEN
    SET batch_size = 2000;
  END IF;
  SELECT * FROM view_macros_automacao ORDER BY data_update ASC, id ASC LIMIT batch_size;
END$$
DELIMITER ;

-- Criar procedimento armazenado para extrair registros finalizados
DELIMITER $$
CREATE PROCEDURE extrair_finalizados()
BEGIN
  -- Selecionar registros da view
  SELECT * FROM view_macros_finalizados;
END$$
DELIMITER ;

-- Trigger to validate and pad CPF with leading zeros
DELIMITER //
CREATE TRIGGER before_insert_clientes
BEFORE INSERT ON clientes
FOR EACH ROW
BEGIN
  IF LENGTH(NEW.cpf) < 11 THEN
    SET NEW.cpf = LPAD(NEW.cpf, 11, '0');
  END IF;
END //
DELIMITER ;

DELIMITER //
CREATE TRIGGER before_update_clientes
BEFORE UPDATE ON clientes
FOR EACH ROW
BEGIN
  IF LENGTH(NEW.cpf) < 11 THEN
    SET NEW.cpf = LPAD(NEW.cpf, 11, '0');
  END IF;
END //
DELIMITER ;

-- Trigger to validate and pad UC with leading zeros
-- UC padding triggers removed (UC is now stored in `cliente_uc`).

-- Create the `cliente_uc` table
CREATE TABLE IF NOT EXISTS cliente_uc (
  id INT NOT NULL AUTO_INCREMENT,
  cliente_id INT NOT NULL,
  uc CHAR(10) NOT NULL,
  distribuidora_id TINYINT UNSIGNED NOT NULL,
  ativo TINYINT(1) DEFAULT 1,
  data_criacao DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY ux_cliente_uc (cliente_id, uc),
  FOREIGN KEY (cliente_id) REFERENCES clientes (id) ON DELETE CASCADE,
  FOREIGN KEY (distribuidora_id) REFERENCES distribuidoras (id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Adicionar chaves estrangeiras na tabela_macro_api agora que as tabelas referenciadas existem
ALTER TABLE tabela_macro_api
  ADD CONSTRAINT fk_macro_api_cliente FOREIGN KEY (cliente_id) REFERENCES clientes (id) ON DELETE CASCADE,
  ADD CONSTRAINT fk_macro_api_cliente_uc FOREIGN KEY (cliente_uc_id) REFERENCES cliente_uc (id) ON DELETE SET NULL,
  ADD CONSTRAINT fk_macro_api_distrib FOREIGN KEY (distribuidora_id) REFERENCES distribuidoras (id) ON DELETE SET NULL,
  ADD CONSTRAINT fk_macro_api_resposta FOREIGN KEY (resposta_id) REFERENCES respostas (id) ON DELETE SET NULL;

-- Índices para `cliente_uc`
ALTER TABLE cliente_uc
  ADD INDEX idx_cliente_uc_cliente_distrib_uc (cliente_id, distribuidora_id, uc),
  ADD INDEX idx_cliente_uc_uc (uc);

-- Minor indexes for lookups
ALTER TABLE clientes
  ADD INDEX idx_clientes_nome (nome(100));

-- Modify the `enderecos` table to reference `cliente_uc`
ALTER TABLE enderecos
MODIFY cliente_id INT DEFAULT NULL,
ADD cliente_uc_id INT NOT NULL,
ADD CONSTRAINT fk_enderecos_cliente_uc FOREIGN KEY (cliente_uc_id) REFERENCES cliente_uc (id) ON DELETE CASCADE;

-- Trigger removed: automatic distribuidora resolution via selecting from `enderecos` would create an FK/ordering conflict
-- Recommendation: determine `distribuidora_id` in application logic or via a stored procedure after both `cliente_uc` and `enderecos` records exist.
