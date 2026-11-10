-- No revierte nada: los lotes completados ya pudieron liberarse y enviarse. Sin esta migración,
-- un lote que siga DISPUTED con la referencia no cambia de comportamiento: el job lo intenta
-- liberar igual que a uno cerrado después de 000007. Las columnas se quitan con 000007 down.
SELECT 1;
