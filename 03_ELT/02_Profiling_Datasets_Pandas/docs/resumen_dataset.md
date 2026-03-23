# Resumen ejecutivo del dataset
- Filas: **50,500**
- Columnas: **15**
- Duplicados exactos: **200**

## Columnas con más nulos
- **ciudad**: 1.78%
- **ingreso_mensual**: 1.2%
- **edad_cliente**: 0.8%
- **canal**: 0.6%
- **email**: 0.6%

## Reglas con más incumplimiento
- **edad_valida_18_100**: 4.56% inválidas
- **email_valido_simple**: 1.09% inválidas
- **precio_positivo**: 0.65% inválidas
- **ingreso_no_negativo**: 0.49% inválidas
- **fecha_no_futura**: 0.41% inválidas

## Observaciones clave
- Hay problemas de consistencia en texto y categorías.
- Existen fechas futuras, valores monetarios inválidos y correos mal formados.
- Se recomienda una fase de estandarización antes de usar el dataset para analítica o modelado.
