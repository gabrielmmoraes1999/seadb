package io.github.gabrielmmoraes1999.seadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExpressionTest {

    @TempDir
    Path dir;

    private Database open() {
        Path file = dir.resolve("banco.db");
        if (!Files.exists(file)) {
            Database.create(file, DatabaseOptions.defaults());
        }
        return Database.open(file, "dba", "dba");
    }

    @Test
    void avaliaCaseCastENulo() {
        try (Database db = open()) {
            db.execute("""
                    CREATE TABLE item (
                        id INTEGER PRIMARY KEY,
                        nome VARCHAR(20),
                        qtd INTEGER,
                        dia DATE,
                        hora TIME,
                        instante TIMESTAMP
                    )
                    """);
            db.execute("""
                    INSERT INTO item (id, nome, qtd, dia, hora, instante) VALUES
                    (1, 'Ana', 2, '2024-03-15', '10:20:30', '2024-03-15 10:20:30'),
                    (2, 'Ana', NULL, NULL, NULL, NULL)
                    """);

            SqlResult simples = db.query("""
                    SELECT CASE qtd WHEN 2 THEN nome ELSE 'outro' END AS rotulo
                    FROM item WHERE id = 1
                    """);
            assertEquals("Ana", simples.rows().get(0).getString("rotulo"));

            SqlResult pesquisado = db.query("""
                    SELECT CASE WHEN qtd IS NULL THEN 'vazio' ELSE nome END AS rotulo
                    FROM item ORDER BY id
                    """);
            assertEquals("Ana", pesquisado.rows().get(0).getString("rotulo"));
            assertEquals("vazio", pesquisado.rows().get(1).getString("rotulo"));

            SqlResult semElse = db.query("SELECT CASE qtd WHEN 9 THEN nome END AS rotulo FROM item WHERE id = 1");
            assertNull(semElse.rows().get(0).get("rotulo"));

            SqlResult nulo = db.query("SELECT NULLIF(nome, 'Ana') AS fora, NULLIF(nome, 'Lia') AS dentro FROM item WHERE id = 1");
            assertNull(nulo.rows().get(0).get("fora"));
            assertEquals("Ana", nulo.rows().get(0).getString("dentro"));

            assertEquals(7, db.query("SELECT CAST('7' AS INTEGER) AS n").rows().get(0).getInt("n"));
            assertNull(db.query("SELECT CAST(NULL AS INTEGER) AS n").rows().get(0).get("n"));
            SeaException cast = assertThrows(SeaException.class, () -> db.query("SELECT CAST('x' AS INTEGER) AS n"));
            assertTrue(cast.getMessage().toLowerCase().contains("convert") || cast.getMessage().toLowerCase().contains("integer"));

            SeaException ramos = assertThrows(SeaException.class,
                    () -> db.query("SELECT CASE WHEN 1 = 1 THEN 1 ELSE 'a' END AS n"));
            assertTrue(ramos.getMessage().contains("CASE branch types do not match"));

            db.execute("BEGIN");
            db.execute("UPDATE item SET nome = CASE WHEN id = 1 THEN 'Lia' ELSE nome END WHERE id = 1");
            db.execute("ROLLBACK");
            assertEquals("Ana", db.query("SELECT nome FROM item WHERE id = 1").rows().get(0).getString("nome"));
        }
    }

    @Test
    void avaliaFuncoesTextoNumeroEData() {
        try (Database db = open()) {
            db.execute("""
                    CREATE TABLE item (
                        id INTEGER PRIMARY KEY,
                        nome VARCHAR(20),
                        qtd INTEGER,
                        valor DOUBLE,
                        dia DATE,
                        hora TIME,
                        instante TIMESTAMP
                    )
                    """);
            db.execute("""
                    INSERT INTO item (id, nome, qtd, valor, dia, hora, instante) VALUES
                    (1, '  Ana  ', -4, 1.6, '2024-03-15', '10:20:30', '2024-03-15 10:20:30')
                    """);

            SqlResult texto = db.query("""
                    SELECT
                        SUBSTRING(nome FROM 3 FOR 3) AS pedaco,
                        SUBSTRING(nome, 3, 3) AS pedaco2,
                        TRIM(nome) AS limpo,
                        LTRIM(nome) AS esquerda,
                        RTRIM(nome) AS direita,
                        REPLACE(TRIM(nome), 'A', 'E') AS troca,
                        POSITION('na' IN nome) AS onde,
                        POSITION('na', nome) AS onde2,
                        POSITION('z' IN nome) AS ausente
                    FROM item
                    """);
            assertEquals("Ana", texto.rows().get(0).getString("pedaco"));
            assertEquals("Ana", texto.rows().get(0).getString("pedaco2"));
            assertEquals("Ana", texto.rows().get(0).getString("limpo"));
            assertEquals("Ana  ", texto.rows().get(0).getString("esquerda"));
            assertEquals("  Ana", texto.rows().get(0).getString("direita"));
            assertEquals("Ena", texto.rows().get(0).getString("troca"));
            assertEquals(4L, texto.rows().get(0).getLong("onde"));
            assertEquals(4L, texto.rows().get(0).getLong("onde2"));
            assertEquals(0L, texto.rows().get(0).getLong("ausente"));

            SqlResult numero = db.query("""
                    SELECT
                        ABS(qtd) AS absoluto,
                        SIGN(qtd) AS sinal,
                        MOD(qtd, 3) AS resto,
                        ROUND(valor) AS inteiro,
                        ROUND(valor, 1) AS escala,
                        CEIL(valor) AS teto,
                        FLOOR(valor) AS chao
                    FROM item
                    """);
            assertEquals(4L, numero.rows().get(0).getLong("absoluto"));
            assertEquals(-1, numero.rows().get(0).getInt("sinal"));
            assertEquals(-1L, numero.rows().get(0).getLong("resto"));
            assertEquals(2.0, numero.rows().get(0).get("inteiro"));
            assertEquals(1.6, numero.rows().get(0).get("escala"));
            assertEquals(2.0, numero.rows().get(0).get("teto"));
            assertEquals(1.0, numero.rows().get(0).get("chao"));

            SqlResult data = db.query("""
                    SELECT
                        EXTRACT(YEAR FROM dia) AS ano,
                        EXTRACT(MONTH FROM dia) AS mes,
                        EXTRACT(DAY FROM instante) AS dia_mes,
                        EXTRACT(HOUR FROM hora) AS hora,
                        EXTRACT(MINUTE FROM instante) AS minuto,
                        EXTRACT(SECOND FROM hora) AS segundo,
                        DATEADD(DAY, 1, dia) AS amanha,
                        DATEDIFF(DAY, dia, DATEADD(DAY, 3, dia)) AS dias
                    FROM item
                    """);
            assertEquals(2024, data.rows().get(0).getInt("ano"));
            assertEquals(3, data.rows().get(0).getInt("mes"));
            assertEquals(15, data.rows().get(0).getInt("dia_mes"));
            assertEquals(10, data.rows().get(0).getInt("hora"));
            assertEquals(20, data.rows().get(0).getInt("minuto"));
            assertEquals(30, data.rows().get(0).getInt("segundo"));
            assertEquals(LocalDate.of(2024, 3, 16), data.rows().get(0).get("amanha"));
            assertEquals(3L, data.rows().get(0).getLong("dias"));

            assertNull(db.query("SELECT SUBSTRING(nome, 1, 1) AS t FROM item WHERE nome IS NULL").rows().isEmpty()
                    ? null
                    : "presente");
            db.execute("INSERT INTO item (id, nome) VALUES (2, NULL)");
            assertNull(db.query("SELECT SUBSTRING(nome, 1, 2) AS t FROM item WHERE id = 2").rows().get(0).get("t"));
            assertNull(db.query("SELECT ABS(qtd) AS n FROM item WHERE id = 2").rows().get(0).get("n"));
            assertNull(db.query("SELECT EXTRACT(YEAR FROM dia) AS n FROM item WHERE id = 2").rows().get(0).get("n"));

            SqlResult filtro = db.query("SELECT id FROM item WHERE POSITION('Ana' IN TRIM(nome)) = 1");
            assertEquals(1, filtro.size());

            db.execute("BEGIN");
            db.execute("UPDATE item SET qtd = ABS(qtd) WHERE id = 1");
            db.execute("ROLLBACK");
            assertEquals(-4, db.query("SELECT qtd FROM item WHERE id = 1").rows().get(0).getInt("qtd"));

            assertEquals(LocalTime.of(11, 20, 30),
                    db.query("SELECT DATEADD(HOUR, 1, hora) AS h FROM item WHERE id = 1").rows().get(0).get("h"));
            assertEquals(LocalDateTime.of(2024, 3, 15, 11, 20, 30),
                    db.query("SELECT DATEADD(HOUR, 1, instante) AS h FROM item WHERE id = 1").rows().get(0).get("h"));
        }
    }
}
