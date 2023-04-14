import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import date, datetime, timedelta
from pyspark.sql.functions import current_date 
import re


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#yesterday_datetime = datetime.now() - timedelta(days=1)
#yesterday_date = yesterday_datetime.strftime('%Y-%m-%d')
today_date = datetime.now().strftime('%Y-%m-%d')

#Definiciones DynamicFrame

Transacciones = glueContext.create_dynamic_frame.from_catalog(
    database="pdp-bi",
    table_name="transacciones_pdp_public_transacciones",
    transformation_ctx="Transacciones",
    additional_options = {"sampleQuery":"select * from public.transacciones where created::text LIKE '"+today_date+"%'","jobBookmarkKeys":["id_trx", "id_tipo_transaccion", "id_comercio", "created", "message_trx", "status_trx", "code_trx" ,"banco_response_code", "banco_code_autorizacion", "referencia_1", "referencia_2", "saldo_antes_trx", "monto", "saldo_despues_trx", "nombre_usuario"],"jobBookmarkKeysSortOrder":"asc"}
)

ComisionesHistorico = glueContext.create_dynamic_frame.from_catalog(
    database="pdp-bi",
    table_name="transacciones_pdp_public_tbl_comisiones_historico",
    transformation_ctx="ComisionesHistorico",
    additional_options = {"sampleQuery":"select * from public.tbl_comisiones_historico where fecha::text LIKE '"+today_date+"%'"}
)

TiposContratoComisiones = (
    glueContext.create_dynamic_frame.from_catalog(
        database="pdp-bi",
        table_name="transacciones_pdp_public_tipos_contrato_comisiones",
        transformation_ctx="TiposContratoComisiones",
    )
)

Autorizadores = glueContext.create_dynamic_frame.from_catalog(
    database="pdp-bi",
    table_name="transacciones_pdp_public_autorizadores",
    transformation_ctx="Autorizadores",
)

TipoComision = glueContext.create_dynamic_frame.from_catalog(
    database="pdp-bi",
    table_name="transacciones_pdp_public_tbl_tipo_comision",
    transformation_ctx="TipoComision",
)

GrupoComercios = glueContext.create_dynamic_frame.from_catalog(
    database="pdp-bi",
    table_name="transacciones_pdp_public_tbl_grupo_comercios",
    transformation_ctx="GrupoComercios",
)

UnionGrupoComercios = glueContext.create_dynamic_frame.from_catalog(
    database="pdp-bi",
    table_name="transacciones_pdp_public_tbl_union_grupo_comercios",
    transformation_ctx="UnionGrupoComercios",
)

TiposOperaciones = glueContext.create_dynamic_frame.from_catalog(
    database="pdp-bi",
    table_name="transacciones_pdp_public_tipos_operaciones",
    transformation_ctx="TiposOperaciones",
)

Comercios = glueContext.create_dynamic_frame.from_catalog(
    database="pdp-bi",
    table_name="transacciones_pdp_public_tbl_comercios",
    transformation_ctx="Comercios",
)

#Transformaciones
JoinComisionTipo = Join.apply(
    frame1=TipoComision,
    frame2=ComisionesHistorico,
    keys1=["pk_id_tipo"],
    keys2=["fk_tipo_comision_historica"],
    transformation_ctx="JoinComisionTipo",
)

SuprimirCamposTipoContrato = DropFields.apply(
    frame=TiposContratoComisiones,
    paths=["rete_fuente", "created_at", "rete_ica", "iva", "estado"],
    transformation_ctx="SuprimirCamposTipoContrato",
)

TransformarAutorizadores = ApplyMapping.apply(
    frame=Autorizadores,
    mappings=[
        ("id_tipo_contrato", "long", "id_contrato", "long"),
        ("id_autorizador", "long", "id_aut", "long"),
        ("nombre_autorizador", "string", "nombre_autorizador", "string"),
    ],
    transformation_ctx="TransformarAutorizadores",
)

TransformarTransacciones = ApplyMapping.apply(
    frame=Transacciones,
    mappings=[
        ("status_trx", "boolean", "status_trx", "boolean"),
        ("created", "timestamp", "created", "string"),
        ("id_convenio", "long", "id_convenio", "long"),
        ("id_trx", "long", "id_trx", "long"),
        ("id_comercio", "long", "id_comercio", "long"),
        ("monto", "decimal", "monto", "decimal"),
        ("id_tipo_transaccion", "long", "id_tipo_transaccion", "long"),
        ("id_usuario", "long", "id_usuario", "long"),
    ],
    transformation_ctx="TransformarTransacciones",
)

JoinUnionGrupoComercios = Join.apply(
    frame1=UnionGrupoComercios,
    frame2=GrupoComercios,
    keys1=["fk_tbl_grupo_comercios"],
    keys2=["pk_tbl_grupo_comercios"],
    transformation_ctx="JoinUnionGrupoComercios",
)

SuprimirCamposTiposOperaciones = DropFields.apply(
    frame=TiposOperaciones,
    paths=["parametros", "tipo_operacion_recaudo"],
    transformation_ctx="SuprimirCamposTiposOperaciones",
)

TransformarComercios = ApplyMapping.apply(
    frame=Comercios,
    mappings=[
        ("razon_social_comercio", "string", "razon_social_comercio", "string"),
        ("estado", "boolean", "estado", "boolean"),
        ("dane_ciudad", "string", "dane_ciudad", "string"),
        ("dane_dpto", "string", "dane_dpto", "string"),
        ("pk_comercio", "long", "pk_comercio", "long"),
        ("direccion_comercio", "string", "direccion_comercio", "string"),
        ("fk_tipo_identificacion", "int", "fk_tipo_identificacion", "int"),
        ("credito_comercio", "decimal", "credito_comercio", "decimal"),
        ("nombre_comercio", "string", "nombre_comercio", "string"),
    ],
    transformation_ctx="TransformarComercios",
)

TransformarComisionTipo = ApplyMapping.apply(
    frame=JoinComisionTipo,
    mappings=[
        ("descripcion", "string", "tipo_comision", "string"),
        ("valor_trx", "decimal", "valor_trx", "decimal"),
        ("id_trx", "int", "id_trx_ch", "int"),
        ("id_tipo_transaccion", "int", "id_tipo_transaccion_ch", "int"),
        ("comision_total", "decimal", "comision_total", "decimal"),
    ],
    transformation_ctx="TransformarComisionTipo",
)

JoinTipoContAut = Join.apply(
    frame1=SuprimirCamposTipoContrato,
    frame2=TransformarAutorizadores,
    keys1=["id_tipo_contrato"],
    keys2=["id_contrato"],
    transformation_ctx="JoinTipoContAut",
)

JoinComercioTipoComercio = Join.apply(
    frame1=TransformarComercios,
    frame2=JoinUnionGrupoComercios,
    keys1=["pk_comercio"],
    keys2=["fk_comercio"],
    transformation_ctx="JoinComercioTipoComercio",
)

JoinTrxTipoOp = Join.apply(
    frame1=SuprimirCamposTiposOperaciones,
    frame2=TransformarTransacciones,
    keys1=["id_tipo_op"],
    keys2=["id_tipo_transaccion"],
    transformation_ctx="JoinTrxTipoOp",
)

JoinTrxTipoOpTipoContAut = Join.apply(
    frame1=JoinTipoContAut,
    frame2=JoinTrxTipoOp,
    keys1=["id_aut"],
    keys2=["id_autorizador"],
    transformation_ctx="JoinTrxTipoOpTipoContAut",
)

Jointrxtipooptipocontaut_node1677707719472DF = (
    JoinTrxTipoOpTipoContAut.toDF()
)
Joincomerciotipocomercio_node1677881137643DF = (
    JoinComercioTipoComercio.toDF()
)
Jointrxtipooptipocontautcomercios_node1677770193532 = DynamicFrame.fromDF(
    Jointrxtipooptipocontaut_node1677707719472DF.join(
        Joincomerciotipocomercio_node1677881137643DF,
        (
            Jointrxtipooptipocontaut_node1677707719472DF["id_comercio"]
            == Joincomerciotipocomercio_node1677881137643DF["pk_comercio"]
        ),
        "left",
    ),
    glueContext,
    "Jointrxtipooptipocontautcomercios_node1677770193532",
)

Transformarcomisiontipo_node1678199147410DF = (
    TransformarComisionTipo.toDF()
)
Jointrxtipooptipocontautcomercios_node1677770193532DF = (
    Jointrxtipooptipocontautcomercios_node1677770193532.toDF()
)
Jointrxtipooptipocontautcomcomhist_node1678199543474 = DynamicFrame.fromDF(
    Transformarcomisiontipo_node1678199147410DF.join(
        Jointrxtipooptipocontautcomercios_node1677770193532DF,
        (
            Transformarcomisiontipo_node1678199147410DF["id_trx_ch"]
            == Jointrxtipooptipocontautcomercios_node1677770193532DF["id_trx"]
        )
        & (
            Transformarcomisiontipo_node1678199147410DF["id_tipo_transaccion_ch"]
            == Jointrxtipooptipocontautcomercios_node1677770193532DF[
                "id_tipo_transaccion"
            ]
        ),
        "right",
    ),
    glueContext,
    "Jointrxtipooptipocontautcomcomhist_node1678199543474",
)

SuprimirIDs = DropFields.apply(
    frame=Jointrxtipooptipocontautcomcomhist_node1678199543474,
    paths=[
        "id_tipo_contrato",
        "id_contrato",
        "id_convenio",
        "id_tipo_transaccion",
        "estado",
        "pk_comercio",
        "fk_tipo_identificacion",
        "id_trx_ch",
        "id_tipo_transaccion_ch",
        "fk_comercio",
        "fk_tbl_grupo_comercios",
        "fk_tbl_grupo_planes_comisiones",
        "pk_tbl_grupo_comercios",
        "fecha_creacion",
        "fecha_modificacion",
        "data_extra",
        "razon_social_comercio",
        "direccion_comercio",
        "credito_comercio",
        "nombre_contrato",
        "paga_comision",
        "id_aut",
    ],
    transformation_ctx="SuprimirIDs",
)

PostgreSQL = glueContext.write_dynamic_frame.from_catalog(
    frame=SuprimirIDs,
    database="pdp-bi-salida",
    table_name="procesado_db_pdp_bi_public_tbl_transacciones",
    transformation_ctx="PostgreSQL",
)

job.commit()