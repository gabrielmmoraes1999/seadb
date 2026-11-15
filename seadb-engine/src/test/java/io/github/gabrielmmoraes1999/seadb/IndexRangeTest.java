package io.github.gabrielmmoraes1999.seadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IndexRangeTest {

    @TempDir
    Path temp;

    @Test
    void faixaLikePrefixoOrEOrdem() throws Exception {
        Path file = temp.resolve("faixa.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Database db = Database.open(file, "dba", "dba")) {
            db.execute("CREATE TABLE item (id INTEGER PRIMARY KEY, nome VARCHAR(40), preco INTEGER)");
            db.execute("CREATE INDEX item_nome ON item (nome)");
            db.execute("CREATE INDEX item_preco ON item (preco) DESCENDING");
            db.execute("INSERT INTO item (id, nome, preco) VALUES (1, 'ana', 10)");
            db.execute("INSERT INTO item (id, nome, preco) VALUES (2, 'bruno', 20)");
            db.execute("INSERT INTO item (id, nome, preco) VALUES (3, 'bia', 30)");
            db.execute("INSERT INTO item (id, nome, preco) VALUES (4, 'carla', 40)");
            assertEquals(List.of("bia", "bruno"), nomes(db, "SELECT nome FROM item WHERE nome >= 'b' AND nome < 'c' ORDER BY nome"));
            assertEquals(List.of("ana", "bia", "bruno"), nomes(db, "SELECT nome FROM item WHERE nome BETWEEN 'ana' AND 'bruno' ORDER BY nome"));
            assertEquals(List.of("bruno"), nomes(db, "SELECT nome FROM item WHERE nome LIKE 'bru%'"));
            assertEquals(List.of("carla"), nomes(db, "SELECT nome FROM item WHERE nome LIKE '%la'"));
            assertEquals(List.of("ana", "carla"), nomes(db, "SELECT nome FROM item WHERE nome = 'ana' OR nome = 'carla' ORDER BY nome"));
            assertEquals(List.of("ana", "bruno"), nomes(db, "SELECT nome FROM item WHERE nome = 'ana' OR preco = 20 ORDER BY nome"));
            assertEquals(List.of("carla", "bruno", "bia", "ana"), nomes(db, "SELECT nome FROM item ORDER BY nome DESC"));
            assertEquals(List.of(40, 30, 20, 10), precos(db, "SELECT preco FROM item ORDER BY preco DESC"));
        }
    }

    private static List<String> nomes(Database db, String sql) {
        List<String> names = new java.util.ArrayList<>();
        for (Row row : db.query(sql).rows()) {
            names.add(row.getString("nome"));
        }
        return names;
    }

    private static List<Integer> precos(Database db, String sql) {
        List<Integer> values = new java.util.ArrayList<>();
        for (Row row : db.query(sql).rows()) {
            values.add(row.getInt("preco"));
        }
        return values;
    }
}
