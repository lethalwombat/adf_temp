param factoryName string = 'exp-edas-dev-datafactory1'

@secure()
param ls_azsqldb_metadatacontroldb_connectionString string = 'Integrated Security=False;Encrypt=True;Connection Timeout=30;Data Source=exp-edas-dev-sqlserver1.database.windows.net;Initial Catalog=controldb'

@secure()
param ls_oracle_connectionString string = 'host=@{linkedService().host};port=@{linkedService().port};sid=@{linkedService().SID};user id=@{linkedService().userName}'

@secure()
param ls_sqlserver_connectionString string = 'Integrated Security=False;Data Source=@{linkedService().serverName};Initial Catalog=@{linkedService().databaseName};User ID=@{linkedService().userName}'

@secure()
param ls_synapsesqlondemand_gen01_connectionString string = 'Integrated Security=False;Encrypt=True;Connection Timeout=30;Data Source=exp-etas-dev-synapse1-ondemand.sql.azuresynapse.net;Initial Catalog=synapsedb'
param ls_azdatalake_properties_typeProperties_serviceEndpoint string = 'https://expetasdev5qmnl6sk7aemc.blob.core.windows.net'
param ls_azkeyvault_properties_typeProperties_baseUrl string = 'https://exp-edas-dev-kv1.vault.azure.net'
param ls_filesystem_properties_typeProperties_host string = '@{linkedService().host}'
param ls_filesystem_properties_typeProperties_userId string = '@{linkedService().userName}'

var factoryId = 'Microsoft.DataFactory/factories/${factoryName}'

resource factoryName_ADF_CETAS_SP 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/ADF_CETAS_SP'
  properties: {
    description: 'The pipeline writes a view to an external table taking view name, table name and schema as parameters. View schema and table schema should be the same.\n\nThe process takes backup of the existing files if it fails the process restores the data.'
    activities: [
      {
        name: 'Initial_Run_Check'
        description: 'Checks if the folder exists, if not it\'s assumed as the initial run for that external table'
        type: 'GetMetadata'
        dependsOn: []
        policy: {
          timeout: '7.00:00:00'
          retry: 0
          retryIntervalInSeconds: 30
          secureOutput: false
          secureInput: false
        }
        userProperties: []
        typeProperties: {
          dataset: {
            referenceName: 'CETAS_Binary_DS'
            type: 'DatasetReference'
            parameters: {
              cetas_Container: {
                value: '@pipeline().parameters.container'
                type: 'Expression'
              }
              cetas_Folder: {
                value: '@if(empty(pipeline().parameters.PipelineStartTime),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/\'),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/\'))'
                type: 'Expression'
              }
            }
          }
          fieldList: [
            'exists'
          ]
          storeSettings: {
            type: 'AzureBlobFSReadSettings'
            recursive: true
            enablePartitionDiscovery: false
          }
          formatSettings: {
            type: 'BinaryReadSettings'
          }
        }
      }
      {
        name: 'Check Folder Exists'
        description: 'If the folder exists it deletes the backup folder takes the backup of existing data files and runs the CETAS DDL. If fails it restores the backup.  If not runs the CETAS, it creates the files.'
        type: 'IfCondition'
        dependsOn: [
          {
            activity: 'Initial_Run_Check'
            dependencyConditions: [
              'Succeeded'
            ]
          }
        ]
        userProperties: []
        typeProperties: {
          expression: {
            value: '@activity(\'Initial_Run_Check\').output.exists'
            type: 'Expression'
          }
          ifFalseActivities: [
            {
              name: 'Call_CETAS_Initial'
              type: 'SqlServerStoredProcedure'
              dependsOn: []
              policy: {
                timeout: '7.00:00:00'
                retry: 0
                retryIntervalInSeconds: 30
                secureOutput: false
                secureInput: false
              }
              userProperties: []
              typeProperties: {
                storedProcedureName: '[helper].[usp_Write_External_Table_From_View]'
                storedProcedureParameters: {
                  DATETIMEPATH: {
                    value: {
                      value: '@if(empty(pipeline().parameters.PipelineStartTime),concat(formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')),concat(formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')))'
                      type: 'Expression'
                    }
                    type: 'String'
                  }
                  SOURCESCHEMA: {
                    value: {
                      value: '@pipeline().parameters.sourceSchema'
                      type: 'Expression'
                    }
                    type: 'String'
                  }
                  STORAGEACCOUNT: {
                    value: {
                      value: '@pipeline().parameters.storageAccount'
                      type: 'Expression'
                    }
                    type: 'String'
                  }
                  TARGETSCHEMA: {
                    value: {
                      value: '@pipeline().parameters.targetSchema'
                      type: 'Expression'
                    }
                    type: 'String'
                  }
                  TB_NAME: {
                    value: {
                      value: '@pipeline().parameters.targetName'
                      type: 'Expression'
                    }
                    type: 'String'
                  }
                  VW_NAME: {
                    value: {
                      value: '@pipeline().parameters.sourceName'
                      type: 'Expression'
                    }
                    type: 'String'
                  }
                  debug: {
                    value: '0'
                    type: 'Int16'
                  }
                }
              }
              linkedServiceName: {
                referenceName: 'ls_synapsesqlondemand_gen01'
                type: 'LinkedServiceReference'
              }
            }
          ]
          ifTrueActivities: [
            {
              name: 'Backup_Data'
              description: 'Backups the data'
              type: 'Copy'
              dependsOn: [
                {
                  activity: 'DeleteExistingBackup'
                  dependencyConditions: [
                    'Completed'
                  ]
                }
              ]
              policy: {
                timeout: '7.00:00:00'
                retry: 0
                retryIntervalInSeconds: 30
                secureOutput: false
                secureInput: false
              }
              userProperties: []
              typeProperties: {
                source: {
                  type: 'BinarySource'
                  storeSettings: {
                    type: 'AzureBlobFSReadSettings'
                    recursive: true
                    wildcardFolderPath: {
                      value: '@if(empty(pipeline().parameters.PipelineStartTime),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/data/\'),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/data/\'))'
                      type: 'Expression'
                    }
                    wildcardFileName: '*'
                    deleteFilesAfterCompletion: true
                  }
                  formatSettings: {
                    type: 'BinaryReadSettings'
                  }
                }
                sink: {
                  type: 'BinarySink'
                  storeSettings: {
                    type: 'AzureBlobFSWriteSettings'
                  }
                }
                enableStaging: false
              }
              inputs: [
                {
                  referenceName: 'CETAS_Binary_DS'
                  type: 'DatasetReference'
                  parameters: {
                    cetas_Container: {
                      value: '@pipeline().parameters.container'
                      type: 'Expression'
                    }
                    cetas_Folder: {
                      value: '@if(empty(pipeline().parameters.PipelineStartTime),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/data/\'),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/data/\'))'
                      type: 'Expression'
                    }
                  }
                }
              ]
              outputs: [
                {
                  referenceName: 'CETAS_Binary_DS'
                  type: 'DatasetReference'
                  parameters: {
                    cetas_Container: {
                      value: '@pipeline().parameters.container'
                      type: 'Expression'
                    }
                    cetas_Folder: {
                      value: '@if(empty(pipeline().parameters.PipelineStartTime),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/backup/\'),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/backup/\'))'
                      type: 'Expression'
                    }
                  }
                }
              ]
            }
            {
              name: 'DeleteExistingBackup'
              description: 'Deletes the backup-it fails if the backup doesn\'t exist but pipeline continues to process. This happens only on the second run.'
              type: 'Delete'
              dependsOn: []
              policy: {
                timeout: '7.00:00:00'
                retry: 0
                retryIntervalInSeconds: 30
                secureOutput: false
                secureInput: false
              }
              userProperties: []
              typeProperties: {
                dataset: {
                  referenceName: 'CETAS_Binary_DS'
                  type: 'DatasetReference'
                  parameters: {
                    cetas_Container: {
                      value: '@pipeline().parameters.container'
                      type: 'Expression'
                    }
                    cetas_Folder: {
                      value: '@if(empty(pipeline().parameters.PipelineStartTime),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/backup/\'),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/backup/\'))'
                      type: 'Expression'
                    }
                  }
                }
                enableLogging: false
                storeSettings: {
                  type: 'AzureBlobFSReadSettings'
                  recursive: true
                  wildcardFileName: '*'
                  enablePartitionDiscovery: false
                }
              }
            }
            {
              name: 'Call_CETAS'
              description: 'Runs the stored procedure with required parameters'
              type: 'SqlServerStoredProcedure'
              dependsOn: [
                {
                  activity: 'DeleteTargetFolder'
                  dependencyConditions: [
                    'Succeeded'
                  ]
                }
              ]
              policy: {
                timeout: '7.00:00:00'
                retry: 0
                retryIntervalInSeconds: 30
                secureOutput: false
                secureInput: false
              }
              userProperties: []
              typeProperties: {
                storedProcedureName: '[helper].[usp_Write_External_Table_From_View]'
                storedProcedureParameters: {
                  DATETIMEPATH: {
                    value: {
                      value: '@if(empty(pipeline().parameters.PipelineStartTime),concat(formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')),concat(formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')))'
                      type: 'Expression'
                    }
                    type: 'String'
                  }
                  SOURCESCHEMA: {
                    value: {
                      value: '@pipeline().parameters.sourceSchema'
                      type: 'Expression'
                    }
                    type: 'String'
                  }
                  STORAGEACCOUNT: {
                    value: {
                      value: '@pipeline().parameters.storageAccount'
                      type: 'Expression'
                    }
                    type: 'String'
                  }
                  TARGETSCHEMA: {
                    value: {
                      value: '@pipeline().parameters.targetSchema'
                      type: 'Expression'
                    }
                    type: 'String'
                  }
                  TB_NAME: {
                    value: {
                      value: '@pipeline().parameters.targetName'
                      type: 'Expression'
                    }
                    type: 'String'
                  }
                  VW_NAME: {
                    value: {
                      value: '@pipeline().parameters.sourceName'
                      type: 'Expression'
                    }
                    type: 'String'
                  }
                  debug: {
                    value: '0'
                    type: 'Int16'
                  }
                }
              }
              linkedServiceName: {
                referenceName: 'ls_synapsesqlondemand_gen01'
                type: 'LinkedServiceReference'
              }
            }
            {
              name: 'Restore_Backup'
              description: 'Upon failure restores the backup files'
              type: 'Copy'
              dependsOn: [
                {
                  activity: 'Call_CETAS'
                  dependencyConditions: [
                    'Failed'
                  ]
                }
              ]
              policy: {
                timeout: '7.00:00:00'
                retry: 0
                retryIntervalInSeconds: 30
                secureOutput: false
                secureInput: false
              }
              userProperties: []
              typeProperties: {
                source: {
                  type: 'BinarySource'
                  storeSettings: {
                    type: 'AzureBlobFSReadSettings'
                    recursive: true
                    wildcardFolderPath: {
                      value: '@if(empty(pipeline().parameters.PipelineStartTime),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/backup/\'),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/backup/\'))'
                      type: 'Expression'
                    }
                    wildcardFileName: '*'
                    deleteFilesAfterCompletion: false
                  }
                  formatSettings: {
                    type: 'BinaryReadSettings'
                  }
                }
                sink: {
                  type: 'BinarySink'
                  storeSettings: {
                    type: 'AzureBlobFSWriteSettings'
                  }
                }
                enableStaging: false
              }
              inputs: [
                {
                  referenceName: 'CETAS_Binary_DS'
                  type: 'DatasetReference'
                  parameters: {
                    cetas_Container: {
                      value: '@pipeline().parameters.container'
                      type: 'Expression'
                    }
                    cetas_Folder: {
                      value: '@if(empty(pipeline().parameters.PipelineStartTime),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/backup/\'),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/backup/\'))'
                      type: 'Expression'
                    }
                  }
                }
              ]
              outputs: [
                {
                  referenceName: 'CETAS_Binary_DS'
                  type: 'DatasetReference'
                  parameters: {
                    cetas_Container: {
                      value: '@pipeline().parameters.container'
                      type: 'Expression'
                    }
                    cetas_Folder: {
                      value: '@if(empty(pipeline().parameters.PipelineStartTime),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/data/\'),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/data/\'))'
                      type: 'Expression'
                    }
                  }
                }
              ]
            }
            {
              name: 'DeleteTargetFolder'
              description: 'Deletes the target folder for CETAS'
              type: 'Delete'
              dependsOn: [
                {
                  activity: 'Backup_Data'
                  dependencyConditions: [
                    'Succeeded'
                  ]
                }
              ]
              policy: {
                timeout: '7.00:00:00'
                retry: 0
                retryIntervalInSeconds: 30
                secureOutput: false
                secureInput: false
              }
              userProperties: []
              typeProperties: {
                dataset: {
                  referenceName: 'CETAS_Binary_DS'
                  type: 'DatasetReference'
                  parameters: {
                    cetas_Container: {
                      value: '@pipeline().parameters.container'
                      type: 'Expression'
                    }
                    cetas_Folder: {
                      value: '@if(empty(pipeline().parameters.PipelineStartTime),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/data/\'),concat(pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\'),\'/data/\'))'
                      type: 'Expression'
                    }
                  }
                }
                enableLogging: false
                storeSettings: {
                  type: 'AzureBlobFSReadSettings'
                  recursive: true
                  enablePartitionDiscovery: false
                }
              }
            }
          ]
        }
      }
    ]
    policy: {
      elapsedTimeMetric: {}
    }
    parameters: {
      targetName: {
        type: 'string'
      }
      targetSchema: {
        type: 'string'
      }
      sourceName: {
        type: 'string'
      }
      sourceSchema: {
        type: 'string'
      }
      container: {
        type: 'string'
      }
      storageAccount: {
        type: 'string'
      }
      PipelineStartTime: {
        type: 'string'
      }
    }
    folder: {
      name: 'SCD/Transformations'
    }
    annotations: []
    lastPublishTime: '2023-11-13T01:40:20Z'
  }
  dependsOn: [
    '${factoryId}/datasets/CETAS_Binary_DS'
    '${factoryId}/linkedServices/ls_synapsesqlondemand_gen01'
  ]
}

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

resource factoryName_CETAS_Binary_DS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/CETAS_Binary_DS'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azdatalake'
      type: 'LinkedServiceReference'
    }
    parameters: {
      cetas_Container: {
        type: 'string'
        defaultValue: 'transformed'
      }
      cetas_Folder: {
        type: 'string'
      }
    }
    folder: {
      name: 'CETAS'
    }
    annotations: []
    type: 'Binary'
    typeProperties: {
      location: {
        type: 'AzureBlobFSLocation'
        folderPath: {
          value: '@dataset().cetas_Folder'
          type: 'Expression'
        }
        fileSystem: {
          value: '@dataset().cetas_Container'
          type: 'Expression'
        }
      }
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}

resource factoryName_EXTPQ_Parquet_DS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/EXTPQ_Parquet_DS'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azdatalake'
      type: 'LinkedServiceReference'
    }
    parameters: {
      extpq_Container: {
        type: 'string'
      }
      extpq_Folder: {
        type: 'string'
      }
      extpq_FileName: {
        type: 'string'
      }
    }
    folder: {
      name: 'EXTPQ'
    }
    annotations: []
    type: 'Parquet'
    typeProperties: {
      location: {
        type: 'AzureBlobFSLocation'
        fileName: {
          value: '@dataset().extpq_FileName'
          type: 'Expression'
        }
        folderPath: {
          value: '@dataset().extpq_Folder'
          type: 'Expression'
        }
        fileSystem: {
          value: '@dataset().extpq_Container'
          type: 'Expression'
        }
      }
      compressionCodec: 'snappy'
    }
    schema: []
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}

resource factoryName_MetadataDrivenCopy_Excel_SourceDS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_Excel_SourceDS'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_filesystem'
      type: 'LinkedServiceReference'
    }
    parameters: {
      cw_worksheetName: {
        type: 'string'
      }
      cw_folderName: {
        type: 'string'
      }
      cw_fileName: {
        type: 'string'
      }
      cw_ls_host: {
        type: 'string'
      }
      cw_ls_userName: {
        type: 'string'
      }
      cw_ls_passwordSecretName: {
        type: 'string'
      }
      cw_range: {
        type: 'string'
      }
      cw_firstRowAsHeaderSource: {
        type: 'bool'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_FileSystem'
    }
    annotations: []
    type: 'Excel'
    typeProperties: {
      sheetName: {
        value: '@dataset().cw_worksheetName'
        type: 'Expression'
      }
      location: {
        type: 'FileServerLocation'
        fileName: {
          value: '@dataset().cw_fileName'
          type: 'Expression'
        }
        folderPath: {
          value: '@dataset().cw_folderName'
          type: 'Expression'
        }
      }
      range: {
        value: '@dataset().cw_range'
        type: 'Expression'
      }
      firstRowAsHeader: {
        value: '@dataset().cw_firstRowAsHeaderSource'
        type: 'Expression'
      }
    }
    schema: []
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_filesystem'
  ]
}

resource factoryName_MetadataDrivenCopy_FileSystem_DestinationDS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_FileSystem_DestinationDS'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azdatalake'
      type: 'LinkedServiceReference'
    }
    parameters: {
      cw_compressionCodec: {
        type: 'String'
      }
      cw_compressionLevel: {
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
      cw_firstRowAsHeader: {
        type: 'Bool'
      }
      cw_fileName: {
        type: 'String'
      }
      cw_folderPath: {
        type: 'String'
      }
      cw_container: {
        type: 'String'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_FileSystem'
    }
    annotations: []
    type: 'DelimitedText'
    typeProperties: {
      location: {
        type: 'AzureBlobFSLocation'
        fileName: {
          value: '@dataset().cw_fileName'
          type: 'Expression'
        }
        folderPath: {
          value: '@concat(dataset().cw_folderPath,\'/\',formatDateTime(convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy/MM/dd\'))'
          type: 'Expression'
        }
        fileSystem: {
          value: '@dataset().cw_container'
          type: 'Expression'
        }
      }
      columnDelimiter: {
        value: '@dataset().cw_columnDelimiter'
        type: 'Expression'
      }
      compressionCodec: {
        value: '@dataset().cw_compressionCodec'
        type: 'Expression'
      }
      compressionLevel: {
        value: '@dataset().cw_compressionLevel'
        type: 'Expression'
      }
      escapeChar: {
        value: '@dataset().cw_escapeChar'
        type: 'Expression'
      }
      firstRowAsHeader: {
        value: '@dataset().cw_firstRowAsHeader'
        type: 'Expression'
      }
      quoteChar: {
        value: '@dataset().cw_quoteChar'
        type: 'Expression'
      }
    }
    schema: []
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}

resource factoryName_MetadataDrivenCopy_FileSystem_SourceDS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_FileSystem_SourceDS'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_filesystem'
      type: 'LinkedServiceReference'
    }
    parameters: {
      cw_worksheetName: {
        type: 'string'
      }
      cw_folderName: {
        type: 'string'
      }
      cw_fileName: {
        type: 'string'
      }
      cw_ls_host: {
        type: 'string'
      }
      cw_ls_userName: {
        type: 'string'
      }
      cw_ls_passwordSecretName: {
        type: 'string'
      }
      cw_range: {
        type: 'string'
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
      cw_firstRowAsHeaderSource: {
        type: 'bool'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_FileSystem'
    }
    annotations: []
    type: 'DelimitedText'
    typeProperties: {
      location: {
        type: 'FileServerLocation'
        fileName: {
          value: '@dataset().cw_fileName'
          type: 'Expression'
        }
        folderPath: {
          value: '@dataset().cw_folderName'
          type: 'Expression'
        }
      }
      columnDelimiter: {
        value: '@dataset().cw_columnDelimiter'
        type: 'Expression'
      }
      escapeChar: {
        value: '@dataset().cw_escapeChar'
        type: 'Expression'
      }
      firstRowAsHeader: {
        value: '@dataset().cw_firstRowAsHeaderSource'
        type: 'Expression'
      }
      quoteChar: {
        value: '@dataset().cw_quoteChar'
        type: 'Expression'
      }
      sheetName: {
        value: '@dataset().cw_worksheetName'
        type: 'Expression'
      }
      range: {
        value: '@dataset().cw_range'
        type: 'Expression'
      }
    }
    schema: []
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_filesystem'
  ]
}

resource factoryName_MetadataDrivenCopy_Oracle_ControlDS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_Oracle_ControlDS'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azsqldb_metadatacontroldb'
      type: 'LinkedServiceReference'
    }
    folder: {
      name: 'MetadataDrivenCopy_Oracle'
    }
    annotations: []
    type: 'AzureSqlTable'
    schema: []
    typeProperties: {
      schema: 'config'
      table: 'OracleControlTable'
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azsqldb_metadatacontroldb'
  ]
}

resource factoryName_MetadataDrivenCopy_Oracle_DestinationDS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_Oracle_DestinationDS'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azdatalake'
      type: 'LinkedServiceReference'
    }
    parameters: {
      cw_compressionCodec: {
        type: 'String'
      }
      cw_compressionLevel: {
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
      cw_firstRowAsHeader: {
        type: 'Bool'
      }
      cw_fileName: {
        type: 'String'
      }
      cw_folderPath: {
        type: 'String'
      }
      cw_container: {
        type: 'String'
      }
      cw_pipelineStartTime: {
        type: 'string'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_Oracle'
    }
    annotations: []
    type: 'DelimitedText'
    typeProperties: {
      location: {
        type: 'AzureBlobFSLocation'
        fileName: {
          value: '@dataset().cw_fileName'
          type: 'Expression'
        }
        folderPath: {
          value: '@if(empty(dataset().cw_pipelineStartTime),concat(dataset().cw_folderPath,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')),concat(dataset().cw_folderPath,\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')))'
          type: 'Expression'
        }
        fileSystem: {
          value: '@dataset().cw_container'
          type: 'Expression'
        }
      }
      columnDelimiter: {
        value: '@dataset().cw_columnDelimiter'
        type: 'Expression'
      }
      compressionCodec: {
        value: '@dataset().cw_compressionCodec'
        type: 'Expression'
      }
      compressionLevel: {
        value: '@dataset().cw_compressionLevel'
        type: 'Expression'
      }
      escapeChar: {
        value: '@dataset().cw_escapeChar'
        type: 'Expression'
      }
      firstRowAsHeader: {
        value: '@dataset().cw_firstRowAsHeader'
        type: 'Expression'
      }
      quoteChar: {
        value: '@dataset().cw_quoteChar'
        type: 'Expression'
      }
    }
    schema: []
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}

resource factoryName_MetadataDrivenCopy_Oracle_ParquetSourceDS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_Oracle_ParquetSourceDS'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azdatalake'
      type: 'LinkedServiceReference'
    }
    parameters: {
      cw_columnDelimiter: {
        type: 'String'
      }
      cw_escapeChar: {
        type: 'String'
      }
      cw_quoteChar: {
        type: 'String'
      }
      cw_firstRowAsHeader: {
        type: 'Bool'
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
      cw_pipelineStartTime: {
        type: 'string'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_Oracle'
    }
    annotations: []
    type: 'DelimitedText'
    typeProperties: {
      location: {
        type: 'AzureBlobFSLocation'
        fileName: {
          value: '@dataset().cw_fileName'
          type: 'Expression'
        }
        folderPath: {
          value: '@if(empty(dataset().cw_pipelineStartTime),concat(dataset().cw_folderPath,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')),concat(dataset().cw_folderPath,\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')))'
          type: 'Expression'
        }
        fileSystem: {
          value: '@dataset().cw_fileSystem'
          type: 'Expression'
        }
      }
      columnDelimiter: {
        value: '@dataset().cw_columnDelimiter'
        type: 'Expression'
      }
      escapeChar: {
        value: '@dataset().cw_escapeChar'
        type: 'Expression'
      }
      firstRowAsHeader: {
        value: '@dataset().cw_firstRowAsHeader'
        type: 'Expression'
      }
      quoteChar: {
        value: '@dataset().cw_quoteChar'
        type: 'Expression'
      }
    }
    schema: []
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}

resource factoryName_MetadataDrivenCopy_Oracle_Schema 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_Oracle_Schema'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azsqldb_metadatacontroldb'
      type: 'LinkedServiceReference'
    }
    parameters: {
      System: {
        type: 'string'
      }
    }
    folder: {
      name: 'Schema'
    }
    annotations: []
    type: 'AzureSqlTable'
    schema: []
    typeProperties: {
      schema: 'config'
      table: {
        value: '@dataset().System'
        type: 'Expression'
      }
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azsqldb_metadatacontroldb'
  ]
}

resource factoryName_MetadataDrivenCopy_Oracle_SourceDS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_Oracle_SourceDS'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_oracle'
      type: 'LinkedServiceReference'
      parameters: {
        host: {
          value: '@dataset().cw_ls_host'
          type: 'Expression'
        }
        port: {
          value: '@dataset().cw_ls_port'
          type: 'Expression'
        }
        SID: {
          value: '@dataset().cw_ls_SID'
          type: 'Expression'
        }
        userName: {
          value: '@dataset().cw_ls_userName'
          type: 'Expression'
        }
        passwordSecretName: {
          value: '@dataset().cw_ls_passwordSecretName'
          type: 'Expression'
        }
      }
    }
    parameters: {
      cw_schema: {
        type: 'String'
      }
      cw_table: {
        type: 'String'
      }
      cw_ls_host: {
        type: 'String'
      }
      cw_ls_port: {
        type: 'String'
      }
      cw_ls_SID: {
        type: 'String'
      }
      cw_ls_userName: {
        type: 'String'
      }
      cw_ls_passwordSecretName: {
        type: 'String'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_Oracle'
    }
    annotations: []
    type: 'OracleTable'
    schema: []
    typeProperties: {
      schema: {
        value: '@dataset().cw_schema'
        type: 'Expression'
      }
      table: {
        value: '@dataset().cw_table'
        type: 'Expression'
      }
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_oracle'
  ]
}

resource factoryName_MetadataDrivenCopy_SQL_ControlDS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_SQL_ControlDS'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azsqldb_metadatacontroldb'
      type: 'LinkedServiceReference'
    }
    folder: {
      name: 'MetadataDrivenCopy_SQL'
    }
    annotations: []
    type: 'AzureSqlTable'
    schema: []
    typeProperties: {
      schema: 'config'
      table: 'SQLControlTable'
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azsqldb_metadatacontroldb'
  ]
}

resource factoryName_MetadataDrivenCopy_SQL_DestinationDS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_SQL_DestinationDS'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azdatalake'
      type: 'LinkedServiceReference'
    }
    parameters: {
      cw_compressionCodec: {
        type: 'String'
      }
      cw_compressionLevel: {
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
      cw_firstRowAsHeader: {
        type: 'Bool'
      }
      cw_fileName: {
        type: 'String'
      }
      cw_folderPath: {
        type: 'string'
      }
      cw_container: {
        type: 'String'
      }
      cw_pipelineStartTime: {
        type: 'string'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_SQL'
    }
    annotations: []
    type: 'DelimitedText'
    typeProperties: {
      location: {
        type: 'AzureBlobFSLocation'
        fileName: {
          value: '@dataset().cw_fileName'
          type: 'Expression'
        }
        folderPath: {
          value: '@if(empty(dataset().cw_pipelineStartTime),concat(dataset().cw_folderPath,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')),concat(dataset().cw_folderPath,\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')))'
          type: 'Expression'
        }
        fileSystem: {
          value: '@dataset().cw_container'
          type: 'Expression'
        }
      }
      columnDelimiter: {
        value: '@dataset().cw_columnDelimiter'
        type: 'Expression'
      }
      compressionCodec: {
        value: '@dataset().cw_compressionCodec'
        type: 'Expression'
      }
      compressionLevel: {
        value: '@dataset().cw_compressionLevel'
        type: 'Expression'
      }
      escapeChar: {
        value: '@dataset().cw_escapeChar'
        type: 'Expression'
      }
      firstRowAsHeader: {
        value: '@dataset().cw_firstRowAsHeader'
        type: 'Expression'
      }
      quoteChar: {
        value: '@dataset().cw_quoteChar'
        type: 'Expression'
      }
    }
    schema: []
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}

resource factoryName_MetadataDrivenCopy_SQL_ParquetDestinationDS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_SQL_ParquetDestinationDS'
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
      cw_pipelineStartTime: {
        type: 'string'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_SQL'
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
          value: '@if(empty(dataset().cw_pipelineStartTime),concat(dataset().cw_folderPath,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')),concat(dataset().cw_folderPath,\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')))'
          type: 'Expression'
        }
        fileSystem: {
          value: '@dataset().cw_fileSystem'
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

resource factoryName_MetadataDrivenCopy_SQL_ParquetSourceDS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_SQL_ParquetSourceDS'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azdatalake'
      type: 'LinkedServiceReference'
    }
    parameters: {
      cw_columnDelimiter: {
        type: 'String'
      }
      cw_escapeChar: {
        type: 'String'
      }
      cw_quoteChar: {
        type: 'String'
      }
      cw_firstRowAsHeader: {
        type: 'Bool'
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
      cw_pipelineStartTime: {
        type: 'string'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_SQL'
    }
    annotations: []
    type: 'DelimitedText'
    typeProperties: {
      location: {
        type: 'AzureBlobFSLocation'
        fileName: {
          value: '@dataset().cw_fileName'
          type: 'Expression'
        }
        folderPath: {
          value: '@if(empty(dataset().cw_pipelineStartTime),concat(dataset().cw_folderPath,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')),concat(dataset().cw_folderPath,\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(dataset().cw_pipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')))'
          type: 'Expression'
        }
        fileSystem: {
          value: '@dataset().cw_fileSystem'
          type: 'Expression'
        }
      }
      columnDelimiter: {
        value: '@dataset().cw_columnDelimiter'
        type: 'Expression'
      }
      escapeChar: {
        value: '@dataset().cw_escapeChar'
        type: 'Expression'
      }
      firstRowAsHeader: {
        value: '@dataset().cw_firstRowAsHeader'
        type: 'Expression'
      }
      quoteChar: {
        value: '@dataset().cw_quoteChar'
        type: 'Expression'
      }
    }
    schema: []
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}

resource factoryName_MetadataDrivenCopy_SQL_Schema 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_SQL_Schema'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azsqldb_metadatacontroldb'
      type: 'LinkedServiceReference'
    }
    parameters: {
      System: {
        type: 'string'
      }
    }
    folder: {
      name: 'Schema'
    }
    annotations: []
    type: 'AzureSqlTable'
    schema: []
    typeProperties: {
      schema: 'config'
      table: {
        value: '@dataset().System'
        type: 'Expression'
      }
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azsqldb_metadatacontroldb'
  ]
}

resource factoryName_MetadataDrivenCopy_SQL_SourceDS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_SQL_SourceDS'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_sqlserver'
      type: 'LinkedServiceReference'
      parameters: {
        serverName: {
          value: '@dataset().cw_ls_serverName'
          type: 'Expression'
        }
        databaseName: {
          value: '@dataset().cw_ls_databaseName'
          type: 'Expression'
        }
        userName: {
          value: '@dataset().cw_ls_userName'
          type: 'Expression'
        }
        passwordSecretName: {
          value: '@dataset().cw_ls_passwordSecretName'
          type: 'Expression'
        }
      }
    }
    parameters: {
      cw_schema: {
        type: 'String'
      }
      cw_table: {
        type: 'String'
      }
      cw_ls_serverName: {
        type: 'String'
      }
      cw_ls_databaseName: {
        type: 'String'
      }
      cw_ls_userName: {
        type: 'String'
      }
      cw_ls_passwordSecretName: {
        type: 'String'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_SQL'
    }
    annotations: []
    type: 'SqlServerTable'
    schema: []
    typeProperties: {
      schema: {
        value: '@dataset().cw_schema'
        type: 'Expression'
      }
      table: {
        value: '@dataset().cw_table'
        type: 'Expression'
      }
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_sqlserver'
  ]
}

resource factoryName_SCD_Binary_DS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/SCD_Binary_DS'
  properties: {
    linkedServiceName: {
      referenceName: 'ls_azdatalake'
      type: 'LinkedServiceReference'
    }
    parameters: {
      scd_Container: {
        type: 'string'
      }
      scd_Folder: {
        type: 'string'
      }
    }
    folder: {
      name: 'SCD'
    }
    annotations: []
    type: 'Binary'
    typeProperties: {
      location: {
        type: 'AzureBlobFSLocation'
        folderPath: {
          value: '@dataset().scd_Folder'
          type: 'Expression'
        }
        fileSystem: {
          value: '@dataset().scd_Container'
          type: 'Expression'
        }
      }
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}

resource factoryName_SCD_Delta_DS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/SCD_Delta_DS'
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
    folder: {
      name: 'SCD'
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