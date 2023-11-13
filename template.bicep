param factoryName string = 'exp-etas-dev-datafactory1'

@secure()
param ls_azsqldb_metadatacontroldb_connectionString string = 'Integrated Security=False;Encrypt=True;Connection Timeout=30;Data Source=exp-etas-dev-sqlserver1.database.windows.net;Initial Catalog=controldb'

@secure()
param ls_oracle_connectionString string = 'host=@{linkedService().host};port=@{linkedService().port};sid=@{linkedService().SID};user id=@{linkedService().userName}'

@secure()
param ls_sqlserver_connectionString string = 'Integrated Security=False;Data Source=@{linkedService().serverName};Initial Catalog=@{linkedService().databaseName};User ID=@{linkedService().userName}'

@secure()
param ls_synapsesqlondemand_gen01_connectionString string = 'Integrated Security=False;Encrypt=True;Connection Timeout=30;Data Source=exp-etas-dev-synapse1-ondemand.sql.azuresynapse.net;Initial Catalog=synapsedb'
param ls_azdatalake_properties_typeProperties_serviceEndpoint string = 'https://expetasdev5qmnl6sk7aemc.blob.core.windows.net'
param ls_azkeyvault_properties_typeProperties_baseUrl string = 'https://exp-etas-dev-kv1.vault.azure.net'
param ls_filesystem_properties_typeProperties_host string = '@{linkedService().host}'
param ls_filesystem_properties_typeProperties_userId string = '@{linkedService().userName}'

var factoryId = 'Microsoft.DataFactory/factories/${factoryName}'

resource factoryName_AuditLogs_DataSource 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/AuditLogs_DataSource'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azsqldb_metadatacontroldb'
      type: 'LinkedServiceReference'
    }
    annotations: []
    type: 'AzureSqlTable'
    schema: [
      {
        name: 'logId'
        type: 'bigint'
        precision: 19
      }
      {
        name: 'sourceType'
        type: 'nvarchar'
      }
      {
        name: 'schema'
        type: 'nvarchar'
      }
      {
        name: 'table'
        type: 'nvarchar'
      }
      {
        name: 'schedule'
        type: 'nvarchar'
      }
      {
        name: 'activity'
        type: 'nvarchar'
      }
      {
        name: 'commenceDateTime'
        type: 'datetime'
        precision: 23
        scale: 3
      }
      {
        name: 'completeDateTime'
        type: 'datetime'
        precision: 23
        scale: 3
      }
      {
        name: 'status'
        type: 'nvarchar'
      }
      {
        name: 'errorText'
        type: 'ntext'
      }
      {
        name: 'rowsCopied'
        type: 'bigint'
        precision: 19
      }
      {
        name: 'elMethod'
        type: 'nvarchar'
      }
      {
        name: 'watermark'
        type: 'nvarchar'
      }
      {
        name: 'runDate'
        type: 'date'
      }
    ]
    typeProperties: {
      schema: 'config'
      table: 'AuditLog'
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azsqldb_metadatacontroldb'
  ]
}

resource factoryName_MetadataDrivenCopy_FileSystem_ParquetDestinationDS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_FileSystem_ParquetDestinationDS'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azdatalake'
      type: 'LinkedServiceReference'
    }
    parameters: {
      cw_compressionCodec: {
        type: 'String'
      }
      cw_columnDelimiter: {
        type: 'String'
      }
      cw_escapeChar: {
        type: 'String'
      }
      cw_quoteChar: {
        type: 'String'
      }
      cw_fileName: {
        type: 'String'
      }
      cw_folderPath: {
        type: 'String'
      }
      cw_fileSystem: {
        type: 'String'
      }
      cw_container: {
        type: 'string'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_FileSystem'
    }
    annotations: []
    type: 'Parquet'
    typeProperties: {
      location: {
        type: 'AzureBlobFSLocation'
        fileName: {
          value: '@dataset().cw_fileName'
          type: 'Expression'
        }
        folderPath: {
          value: '@concat(dataset().cw_fileSystem,\'/\',dataset().cw_folderPath,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy/MM/dd\'))'
          type: 'Expression'
        }
        fileSystem: {
          value: '@dataset().cw_container'
          type: 'Expression'
        }
      }
      compressionCodec: {
        value: '@dataset().cw_compressionCodec'
        type: 'Expression'
      }
      columnDelimiter: {
        type: 'Expression'
        value: '@dataset().cw_columnDelimiter'
      }
      escapeChar: {
        type: 'Expression'
        value: '@dataset().cw_escapeChar'
      }
      quoteChar: {
        type: 'Expression'
        value: '@dataset().cw_quoteChar'
      }
    }
    schema: []
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}

resource factoryName_ds_azsqldb_sqldbcontroldb 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/ds_azsqldb_sqldbcontroldb'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azsqldb_metadatacontroldb'
      type: 'LinkedServiceReference'
    }
    annotations: []
    type: 'AzureSqlTable'
    schema: [
      {
        name: 'id'
        type: 'int'
        precision: 10
      }
      {
        name: 'transformation'
        type: 'nvarchar'
      }
      {
        name: 'sourceSchema'
        type: 'nvarchar'
      }
      {
        name: 'sourceObject'
        type: 'nvarchar'
      }
      {
        name: 'targetSchema'
        type: 'nvarchar'
      }
      {
        name: 'targetObject'
        type: 'nvarchar'
      }
      {
        name: 'triggerName'
        type: 'nvarchar'
      }
      {
        name: 'copyEnabled'
        type: 'bit'
      }
    ]
    typeProperties: {
      schema: 'config'
      table: 'MaterialisedTransform'
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azsqldb_metadatacontroldb'
  ]
}

resource factoryName_ds_edaDelta 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/ds_edaDelta'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azdatalake'
      type: 'LinkedServiceReference'
    }
    parameters: {
      targetName: {
        type: 'string'
      }
      targetSchema: {
        type: 'string'
      }
    }
    annotations: []
    type: 'Parquet'
    typeProperties: {
      location: {
        type: 'AzureBlobFSLocation'
        folderPath: {
          value: '@concat(\'Delta/\',dataset().targetSchema,\'/\',dataset().targetName)'
          type: 'Expression'
        }
        fileSystem: 'transformed'
      }
      compressionCodec: 'snappy'
    }
    schema: []
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}

resource factoryName_ds_synapseanalyticsdb 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/ds_synapseanalyticsdb'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_synapsesqlondemand_gen01'
      type: 'LinkedServiceReference'
    }
    annotations: []
    type: 'AzureSqlDWTable'
    schema: []
    typeProperties: {}
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_synapsesqlondemand_gen01'
  ]
}

resource factoryName_ls_azdatalake 'Microsoft.DataFactory/factories/linkedServices@2018-06-01' = {
  name: '${factoryName}/ls_azdatalake'
  properties: {
    description: 'Storage Account Linked Service for ADLS'
    annotations: []
    type: 'AzureBlobStorage'
    typeProperties: {
      serviceEndpoint: ls_azdatalake_properties_typeProperties_serviceEndpoint
      accountKind: 'StorageV2'
    }
  }
  dependsOn: []
}

resource factoryName_ls_azkeyvault 'Microsoft.DataFactory/factories/linkedServices@2018-06-01' = {
  name: '${factoryName}/ls_azkeyvault'
  properties: {
    description: 'Linked Service to Key Vault for secret management'
    annotations: []
    type: 'AzureKeyVault'
    typeProperties: {
      baseUrl: ls_azkeyvault_properties_typeProperties_baseUrl
    }
  }
  dependsOn: []
}

resource factoryName_ls_azsqldb_metadatacontroldb 'Microsoft.DataFactory/factories/linkedServices@2018-06-01' = {
  name: '${factoryName}/ls_azsqldb_metadatacontroldb'
  properties: {
    description: 'Linked Service for Azure Data Factory (ADFv2) configuration items, for metadata driven EL.'
    annotations: []
    type: 'AzureSqlDatabase'
    typeProperties: {
      connectionString: ls_azsqldb_metadatacontroldb_connectionString
    }
  }
  dependsOn: []
}

resource factoryName_ls_filesystem 'Microsoft.DataFactory/factories/linkedServices@2018-06-01' = {
  name: '${factoryName}/ls_filesystem'
  properties: {
    description: 'Generic Oracle linked service for Filesystem Sources'
    parameters: {
      host: {
        type: 'string'
      }
      userName: {
        type: 'string'
      }
      passwordSecretName: {
        type: 'string'
      }
    }
    annotations: []
    type: 'FileServer'
    typeProperties: {
      host: ls_filesystem_properties_typeProperties_host
      userId: ls_filesystem_properties_typeProperties_userId
      password: {
        type: 'AzureKeyVaultSecret'
        store: {
          referenceName: 'ls_azkeyvault'
          type: 'LinkedServiceReference'
        }
        secretName: {
          value: '@linkedService().passwordSecretName'
          type: 'Expression'
        }
      }
    }
    connectVia: {
      referenceName: 'SelfHostedIntegrationRuntime'
      type: 'IntegrationRuntimeReference'
    }
  }
  dependsOn: [
    '${factoryId}/integrationRuntimes/SelfHostedIntegrationRuntime'
    '${factoryId}/linkedServices/ls_azkeyvault'
  ]
}

resource factoryName_ls_oracle 'Microsoft.DataFactory/factories/linkedServices@2018-06-01' = {
  name: '${factoryName}/ls_oracle'
  properties: {
    description: 'Generic Oracle linked service for Oracle Sources'
    parameters: {
      host: {
        type: 'string'
      }
      port: {
        type: 'string'
      }
      SID: {
        type: 'string'
      }
      userName: {
        type: 'string'
      }
      passwordSecretName: {
        type: 'string'
      }
    }
    annotations: []
    type: 'Oracle'
    typeProperties: {
      connectionString: ls_oracle_connectionString
      password: {
        type: 'AzureKeyVaultSecret'
        store: {
          referenceName: 'ls_azkeyvault'
          type: 'LinkedServiceReference'
        }
        secretName: {
          value: '@linkedService().passwordSecretName'
          type: 'Expression'
        }
      }
    }
    connectVia: {
      referenceName: 'SelfHostedIntegrationRuntime'
      type: 'IntegrationRuntimeReference'
    }
  }
  dependsOn: [
    '${factoryId}/integrationRuntimes/SelfHostedIntegrationRuntime'
    '${factoryId}/linkedServices/ls_azkeyvault'
  ]
}

resource factoryName_ls_sqlserver 'Microsoft.DataFactory/factories/linkedServices@2018-06-01' = {
  name: '${factoryName}/ls_sqlserver'
  properties: {
    description: 'Generic SQL Server linked service for SQL Connections (Sources)'
    parameters: {
      serverName: {
        type: 'string'
      }
      databaseName: {
        type: 'string'
      }
      userName: {
        type: 'string'
      }
      passwordSecretName: {
        type: 'string'
      }
    }
    annotations: []
    type: 'SqlServer'
    typeProperties: {
      connectionString: ls_sqlserver_connectionString
      password: {
        type: 'AzureKeyVaultSecret'
        store: {
          referenceName: 'ls_azkeyvault'
          type: 'LinkedServiceReference'
        }
        secretName: {
          value: '@linkedService().passwordSecretName'
          type: 'Expression'
        }
      }
    }
    connectVia: {
      referenceName: 'SelfHostedIntegrationRuntime'
      type: 'IntegrationRuntimeReference'
    }
  }
  dependsOn: [
    '${factoryId}/integrationRuntimes/SelfHostedIntegrationRuntime'
    '${factoryId}/linkedServices/ls_azkeyvault'
  ]
}

resource factoryName_ls_synapsesqlondemand_gen01 'Microsoft.DataFactory/factories/linkedServices@2018-06-01' = {
  name: '${factoryName}/ls_synapsesqlondemand_gen01'
  properties: {
    annotations: []
    type: 'AzureSqlDW'
    typeProperties: {
      connectionString: ls_synapsesqlondemand_gen01_connectionString
    }
  }
  dependsOn: []
}

resource factoryName_SelfHostedIntegrationRuntime 'Microsoft.DataFactory/factories/integrationRuntimes@2018-06-01' = {
  name: '${factoryName}/SelfHostedIntegrationRuntime'
  properties: {
    type: 'SelfHosted'
    typeProperties: {}
  }
  dependsOn: []
}