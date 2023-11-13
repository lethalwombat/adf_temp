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

resource factoryName_ADF_EXTPQ_TF 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/ADF_EXTPQ_TF'
  properties: {
    activities: [
      {
        name: 'CopyTVF'
        type: 'Copy'
        dependsOn: []
        policy: {
          timeout: '0.12:00:00'
          retry: 0
          retryIntervalInSeconds: 30
          secureOutput: false
          secureInput: false
        }
        userProperties: []
        typeProperties: {
          source: {
            type: 'SqlDWSource'
            sqlReaderQuery: {
              value: '@concat(\'select * from \', pipeline().parameters.sourceSchema, \'.\', pipeline().parameters.sourceName, \'(\', pipeline().parameters.parameter, \')\')'
              type: 'Expression'
            }
            queryTimeout: '02:00:00'
            partitionOption: 'None'
          }
          sink: {
            type: 'ParquetSink'
            storeSettings: {
              type: 'AzureBlobFSWriteSettings'
            }
            formatSettings: {
              type: 'ParquetWriteSettings'
            }
          }
          enableStaging: false
          translator: {
            type: 'TabularTranslator'
            typeConversion: true
            typeConversionSettings: {
              allowDataTruncation: true
              treatBooleanAsNumber: false
            }
          }
        }
        inputs: [
          {
            referenceName: 'ds_synapseanalyticsdb'
            type: 'DatasetReference'
            parameters: {}
          }
        ]
        outputs: [
          {
            referenceName: 'EXTPQ_Parquet_DS'
            type: 'DatasetReference'
            parameters: {
              extpq_Container: {
                value: '@pipeline().parameters.container'
                type: 'Expression'
              }
              extpq_Folder: {
                value: '@if(empty(pipeline().parameters.PipelineStartTime),concat(pipeline().parameters.folderName,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')), concat(pipeline().parameters.folderName,\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')))'
                type: 'Expression'
              }
              extpq_FileName: {
                value: '@concat(toLower(pipeline().parameters.targetName), \'.parquet\')'
                type: 'Expression'
              }
            }
          }
        ]
      }
      {
        name: 'Create_External_Table'
        type: 'SqlServerStoredProcedure'
        dependsOn: [
          {
            activity: 'CopyTVF'
            dependencyConditions: [
              'Succeeded'
            ]
          }
        ]
        policy: {
          timeout: '0.12:00:00'
          retry: 0
          retryIntervalInSeconds: 30
          secureOutput: false
          secureInput: false
        }
        userProperties: []
        typeProperties: {
          storedProcedureName: '[helper].[usp_Write_External_Table_On_Entities]'
          storedProcedureParameters: {
            CONTAINER: {
              value: {
                value: '@pipeline().parameters.container'
                type: 'Expression'
              }
              type: 'String'
            }
            DATETIMEPATH: {
              value: {
                value: '@if(empty(pipeline().parameters.PipelineStartTime),concat(formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')),concat(formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')))'
                type: 'Expression'
              }
              type: 'String'
            }
            FOLDERNAME: {
              value: {
                value: '@pipeline().parameters.folderName'
                type: 'Expression'
              }
              type: 'String'
            }
            PARAMETER: {
              value: {
                value: '@pipeline().parameters.parameter'
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
            TABLEPREFIX: {
              value: {
                value: '@pipeline().parameters.tablePrefix'
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
          }
        }
        linkedServiceName: {
          referenceName: 'ls_synapsesqlondemand_gen01'
          type: 'LinkedServiceReference'
        }
      }
    ]
    policy: {
      elapsedTimeMetric: {}
    }
    parameters: {
      targetName: {
        type: 'string'
        defaultValue: 'Address'
      }
      targetSchema: {
        type: 'string'
        defaultValue: 'bus_mdl_extract'
      }
      sourceName: {
        type: 'string'
        defaultValue: 'tf_Address'
      }
      sourceSchema: {
        type: 'string'
        defaultValue: 'bus_mdl'
      }
      database: {
        type: 'string'
        defaultValue: 'satac-dai-hub'
      }
      container: {
        type: 'string'
        defaultValue: 'transformed'
      }
      parameter: {
        type: 'string'
        defaultValue: 'TAFE SA'
      }
      folderName: {
        type: 'string'
        defaultValue: 'All Applicants'
      }
      tablePrefix: {
        type: 'string'
        defaultValue: 'allappz'
      }
      storageAccount: {
        type: 'string'
        defaultValue: 'sataccldaidlsdev01'
      }
      PipelineStartTime: {
        type: 'string'
      }
    }
    folder: {
      name: 'SCD/Transformations'
    }
    annotations: []
  }
  dependsOn: [
    '${factoryId}/datasets/ds_synapseanalyticsdb'
    '${factoryId}/datasets/EXTPQ_Parquet_DS'
    '${factoryId}/linkedServices/ls_synapsesqlondemand_gen01'
  ]
}

resource factoryName_AuditLogs 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/AuditLogs'
  properties: {
    activities: [
      {
        name: 'PartitionLoadAuditLogs'
        description: 'Copy the changed data only from last time via comparing the value in watermark column to identify changes.'
        type: 'Copy'
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
          source: {
            type: 'AzureSqlSource'
            sqlReaderQuery: {
              value: 'SELECT [logId]\n      ,[sourceType]\n      ,[schema]\n      ,[table]\n      ,[schedule]\n      ,[activity]\n      ,[commenceDateTime]\n      ,[completeDateTime]\n      ,[status]\n      ,[errorText]\n      ,[rowsCopied]\n      ,[elMethod]\n      ,[watermark]\n      ,[runDate]\n  FROM [config].[AuditLog]\n  WHERE [runDate] = cast(getdate() as date);'
              type: 'Expression'
            }
            queryTimeout: '02:00:00'
            partitionOption: 'None'
          }
          sink: {
            type: 'ParquetSink'
            storeSettings: {
              type: 'AzureBlobFSWriteSettings'
            }
            formatSettings: {
              type: 'ParquetWriteSettings'
            }
          }
          enableStaging: false
          validateDataConsistency: true
          translator: {
            type: 'TabularTranslator'
            mappings: [
              {
                source: {
                  name: 'logId'
                  type: 'Int64'
                }
                sink: {
                  name: 'logId'
                  type: 'Int64'
                }
              }
              {
                source: {
                  name: 'sourceType'
                  type: 'String'
                }
                sink: {
                  name: 'sourceType'
                  type: 'String'
                }
              }
              {
                source: {
                  name: 'schema'
                  type: 'String'
                }
                sink: {
                  name: 'schema'
                  type: 'String'
                }
              }
              {
                source: {
                  name: 'table'
                  type: 'String'
                }
                sink: {
                  name: 'table'
                  type: 'String'
                }
              }
              {
                source: {
                  name: 'schedule'
                  type: 'String'
                }
                sink: {
                  name: 'schedule'
                  type: 'String'
                }
              }
              {
                source: {
                  name: 'activity'
                  type: 'String'
                }
                sink: {
                  name: 'activity'
                  type: 'String'
                }
              }
              {
                source: {
                  name: 'commenceDateTime'
                  type: 'DateTime'
                }
                sink: {
                  name: 'commenceDateTime'
                  type: 'DateTime'
                }
              }
              {
                source: {
                  name: 'completeDateTime'
                  type: 'DateTime'
                }
                sink: {
                  name: 'completeDateTime'
                  type: 'DateTime'
                }
              }
              {
                source: {
                  name: 'status'
                  type: 'String'
                }
                sink: {
                  name: 'status'
                  type: 'String'
                }
              }
              {
                source: {
                  name: 'errorText'
                  type: 'String'
                }
                sink: {
                  name: 'errorText'
                  type: 'String'
                }
              }
              {
                source: {
                  name: 'rowsCopied'
                  type: 'Int64'
                }
                sink: {
                  name: 'rowsCopied'
                  type: 'Int64'
                }
              }
              {
                source: {
                  name: 'elMethod'
                  type: 'String'
                }
                sink: {
                  name: 'elMethod'
                  type: 'String'
                }
              }
              {
                source: {
                  name: 'watermark'
                  type: 'String'
                }
                sink: {
                  name: 'watermark'
                  type: 'String'
                }
              }
              {
                source: {
                  name: 'runDate'
                  type: 'DateTime'
                }
                sink: {
                  name: 'runDate'
                  type: 'DateTime'
                }
              }
            ]
          }
        }
        inputs: [
          {
            referenceName: 'AuditLogs_DataSource'
            type: 'DatasetReference'
            parameters: {}
          }
        ]
        outputs: [
          {
            referenceName: 'MetadataDrivenCopy_SQL_ParquetDestinationDS'
            type: 'DatasetReference'
            parameters: {
              cw_compressionCodec: 'snappy'
              cw_columnDelimiter: '|'
              cw_escapeChar: '\\'
              cw_quoteChar: '"'
              cw_fileName: 'AuditLogs'
              cw_folderPath: 'metadata\\AuditLogs'
              cw_fileSystem: 'statenet-raw-optimised'
              cw_pipelineStartTime: {
                value: '@pipeline().TriggerTime'
                type: 'Expression'
              }
            }
          }
        ]
      }
    ]
    policy: {
      elapsedTimeMetric: {}
    }
    folder: {
      name: 'Metadata'
    }
    annotations: []
  }
  dependsOn: [
    '${factoryId}/datasets/AuditLogs_DataSource'
    '${factoryId}/datasets/MetadataDrivenCopy_SQL_ParquetDestinationDS'
  ]
}

resource factoryName_MDF_SCD1_SP 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/MDF_SCD1_SP'
  properties: {
    description: 'Creates or updates Delta files, based on SCD Type 1 logic.  Accepts parameters for the source, and target object and schema.  Creates a view of the resultant delta directory at first execution.'
    activities: [
      {
        name: 'Check If Delta Folder Exists'
        description: 'Checks to see whether a Delta file exists, based on the parsed parameters.'
        type: 'GetMetadata'
        dependsOn: []
        policy: {
          timeout: '0.00:05:00'
          retry: 1
          retryIntervalInSeconds: 30
          secureOutput: false
          secureInput: false
        }
        userProperties: []
        typeProperties: {
          dataset: {
            referenceName: 'ds_edaDelta'
            type: 'DatasetReference'
            parameters: {
              targetName: {
                value: '@pipeline().parameters.targetName'
                type: 'Expression'
              }
              targetSchema: {
                value: '@pipeline().parameters.targetSchema'
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
        }
      }
      {
        name: 'If Delta Exists Condition'
        description: 'If condition: Conditionally splits activities, based on whether delta files exist at the target.  If they do not, an initial delta hierarchy and view are created.  If they are, a merge-update is performed based on the incoming data, to the sink.'
        type: 'IfCondition'
        dependsOn: [
          {
            activity: 'Check If Delta Folder Exists'
            dependencyConditions: [
              'Succeeded'
            ]
          }
        ]
        userProperties: []
        typeProperties: {
          expression: {
            value: '@activity(\'Check If Delta Folder Exists\').output.exists'
            type: 'Expression'
          }
          ifFalseActivities: [
            {
              name: 'Data flow_Insert'
              description: 'Creates, and executes an insert to the target directory, based on the provided parameters.'
              type: 'ExecuteDataFlow'
              dependsOn: []
              policy: {
                timeout: '1.00:00:00'
                retry: 0
                retryIntervalInSeconds: 30
                secureOutput: false
                secureInput: false
              }
              userProperties: []
              typeProperties: {
                dataFlow: {
                  referenceName: 'MDF_SCD1_Initial_SP'
                  type: 'DataFlowReference'
                  parameters: {
                    sourceName: {
                      value: '\'@{pipeline().parameters.sourceName}\''
                      type: 'Expression'
                    }
                    sourceSchema: {
                      value: '\'@{pipeline().parameters.sourceSchema}\''
                      type: 'Expression'
                    }
                    targetName: {
                      value: '\'@{pipeline().parameters.targetName}\''
                      type: 'Expression'
                    }
                    targetSchema: {
                      value: '\'@{pipeline().parameters.targetSchema}\''
                      type: 'Expression'
                    }
                  }
                  datasetParameters: {
                    synSslSrcQry: {
                      targetName: {
                        value: '@pipeline().parameters.targetName'
                        type: 'Expression'
                      }
                      targetSchema: {
                        value: '@pipeline().parameters.targetSchema'
                        type: 'Expression'
                      }
                    }
                    sinkDelta: {}
                  }
                }
                staging: {}
                compute: {
                  coreCount: 8
                  computeType: 'General'
                }
                traceLevel: 'Fine'
              }
            }
            {
              name: 'CreateDeltaView'
              description: 'Creates a Delta view of the object directory, based on the provided parameters.'
              type: 'Script'
              dependsOn: [
                {
                  activity: 'Data flow_Insert'
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
              linkedServiceName: {
                referenceName: 'ls_synapsesqlondemand_gen01'
                type: 'LinkedServiceReference'
              }
              typeProperties: {
                scripts: [
                  {
                    type: 'Query'
                    text: {
                      value: '@concat(\'CREATE OR ALTER VIEW [\',pipeline().parameters.targetSchema,\'].[\',pipeline().parameters.targetName,\']\nAS\nSELECT *\nFROM\n    OPENROWSET(\n        BULK \'\'transformed/Delta/\',pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'\'\',\n        DATA_SOURCE = \'\'eds_eduschedadls01_mi\'\',\n        FORMAT = \'\'DELTA\'\'\n    ) AS [result];\')'
                      type: 'Expression'
                    }
                  }
                ]
              }
            }
          ]
          ifTrueActivities: [
            {
              name: 'Data flow_Update'
              description: 'Execute a merge (update), based on the provided parameters.'
              type: 'ExecuteDataFlow'
              dependsOn: []
              policy: {
                timeout: '1.00:00:00'
                retry: 0
                retryIntervalInSeconds: 30
                secureOutput: false
                secureInput: false
              }
              userProperties: []
              typeProperties: {
                dataFlow: {
                  referenceName: 'MDF_SCD1_Update_SP'
                  type: 'DataFlowReference'
                  parameters: {
                    sourceName: {
                      value: '\'@{pipeline().parameters.sourceName}\''
                      type: 'Expression'
                    }
                    sourceSchema: {
                      value: '\'@{pipeline().parameters.sourceSchema}\''
                      type: 'Expression'
                    }
                    targetName: {
                      value: '\'@{pipeline().parameters.targetName}\''
                      type: 'Expression'
                    }
                    targetSchema: {
                      value: '\'@{pipeline().parameters.targetSchema}\''
                      type: 'Expression'
                    }
                  }
                  datasetParameters: {
                    synSslSrcQry: {}
                    synSqlCur: {}
                    sinkDelta: {}
                  }
                }
                staging: {}
                compute: {
                  coreCount: 8
                  computeType: 'General'
                }
                traceLevel: 'Fine'
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
        defaultValue: 'empty'
      }
      targetSchema: {
        type: 'string'
        defaultValue: 'fnd_rel'
      }
      sourceName: {
        type: 'string'
        defaultValue: 'vw_empty_hash'
      }
      sourceSchema: {
        type: 'string'
        defaultValue: 'dbo'
      }
      database: {
        type: 'string'
        defaultValue: 'satac-dai-hub'
      }
    }
    folder: {
      name: 'SCD/Transformations'
    }
    annotations: []
  }
  dependsOn: [
    '${factoryId}/datasets/ds_edaDelta'
    '${factoryId}/dataflows/MDF_SCD1_Initial_SP'
    '${factoryId}/linkedServices/ls_synapsesqlondemand_gen01'
    '${factoryId}/dataflows/MDF_SCD1_Update_SP'
  ]
}

resource factoryName_MDF_SCD1_VW 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/MDF_SCD1_VW'
  properties: {
    description: 'Creates or updates Delta files, based on SCD Type 1 logic.  Accepts parameters for the source, and target object and schema.  Creates a view of the resultant delta directory at first execution.'
    activities: [
      {
        name: 'Check If Delta Folder Exists'
        description: 'Checks to see whether a Delta file exists, based on the parsed parameters.'
        type: 'GetMetadata'
        dependsOn: []
        policy: {
          timeout: '0.00:05:00'
          retry: 1
          retryIntervalInSeconds: 30
          secureOutput: false
          secureInput: false
        }
        userProperties: []
        typeProperties: {
          dataset: {
            referenceName: 'ds_edaDelta'
            type: 'DatasetReference'
            parameters: {
              targetName: {
                value: '@pipeline().parameters.targetName'
                type: 'Expression'
              }
              targetSchema: {
                value: '@pipeline().parameters.targetSchema'
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
        }
      }
      {
        name: 'If Delta Exists Condition'
        description: 'If condition: Conditionally splits activities, based on whether delta files exist at the target.  If they do not, an initial delta hierarchy and view are created.  If they are, a merge-update is performed based on the incoming data, to the sink.'
        type: 'IfCondition'
        dependsOn: [
          {
            activity: 'Check If Delta Folder Exists'
            dependencyConditions: [
              'Succeeded'
            ]
          }
        ]
        userProperties: []
        typeProperties: {
          expression: {
            value: '@activity(\'Check If Delta Folder Exists\').output.exists'
            type: 'Expression'
          }
          ifFalseActivities: [
            {
              name: 'Data flow_Insert'
              description: 'Creates, and executes an insert to the target directory, based on the provided parameters.'
              type: 'ExecuteDataFlow'
              dependsOn: []
              policy: {
                timeout: '1.00:00:00'
                retry: 0
                retryIntervalInSeconds: 30
                secureOutput: false
                secureInput: false
              }
              userProperties: []
              typeProperties: {
                dataFlow: {
                  referenceName: 'MDF_SCD1_Initial_VW'
                  type: 'DataFlowReference'
                  parameters: {
                    sourceName: {
                      value: '\'@{pipeline().parameters.sourceName}\''
                      type: 'Expression'
                    }
                    sourceSchema: {
                      value: '\'@{pipeline().parameters.sourceSchema}\''
                      type: 'Expression'
                    }
                    targetName: {
                      value: '\'@{pipeline().parameters.targetName}\''
                      type: 'Expression'
                    }
                    targetSchema: {
                      value: '\'@{pipeline().parameters.targetSchema}\''
                      type: 'Expression'
                    }
                  }
                  datasetParameters: {
                    synSslSrcQry: {
                      targetName: {
                        value: '@pipeline().parameters.targetName'
                        type: 'Expression'
                      }
                      targetSchema: {
                        value: '@pipeline().parameters.targetSchema'
                        type: 'Expression'
                      }
                    }
                    sinkDelta: {}
                  }
                }
                staging: {}
                compute: {
                  coreCount: 8
                  computeType: 'General'
                }
                traceLevel: 'Fine'
              }
            }
            {
              name: 'CreateDeltaView'
              description: 'Creates a Delta view of the object directory, based on the provided parameters.'
              type: 'Script'
              dependsOn: [
                {
                  activity: 'Data flow_Insert'
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
              linkedServiceName: {
                referenceName: 'ls_synapsesqlondemand_gen01'
                type: 'LinkedServiceReference'
              }
              typeProperties: {
                scripts: [
                  {
                    type: 'Query'
                    text: {
                      value: '@concat(\'CREATE OR ALTER VIEW [\',pipeline().parameters.targetSchema,\'].[\',pipeline().parameters.targetName,\']\nAS\nSELECT *\nFROM\n    OPENROWSET(\n        BULK \'\'transformed/Delta/\',pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'\'\',\n        DATA_SOURCE = \'\'eds_eduschedadls01_mi\'\',\n        FORMAT = \'\'DELTA\'\'\n    ) AS [result];\')'
                      type: 'Expression'
                    }
                  }
                ]
              }
            }
          ]
          ifTrueActivities: [
            {
              name: 'Data flow_Update'
              description: 'Execute a merge (update), based on the provided parameters.'
              type: 'ExecuteDataFlow'
              dependsOn: []
              policy: {
                timeout: '1.00:00:00'
                retry: 0
                retryIntervalInSeconds: 30
                secureOutput: false
                secureInput: false
              }
              userProperties: []
              typeProperties: {
                dataFlow: {
                  referenceName: 'MDF_SCD1_Update_VW'
                  type: 'DataFlowReference'
                  parameters: {
                    sourceName: {
                      value: '\'@{pipeline().parameters.sourceName}\''
                      type: 'Expression'
                    }
                    sourceSchema: {
                      value: '\'@{pipeline().parameters.sourceSchema}\''
                      type: 'Expression'
                    }
                    targetName: {
                      value: '\'@{pipeline().parameters.targetName}\''
                      type: 'Expression'
                    }
                    targetSchema: {
                      value: '\'@{pipeline().parameters.targetSchema}\''
                      type: 'Expression'
                    }
                  }
                  datasetParameters: {
                    synSslSrcQry: {
                      targetName: {
                        value: '@pipeline().parameters.targetName'
                        type: 'Expression'
                      }
                      targetSchema: {
                        value: '@pipeline().parameters.targetSchema'
                        type: 'Expression'
                      }
                    }
                    synSqlCur: {
                      targetName: {
                        value: '@pipeline().parameters.targetName'
                        type: 'Expression'
                      }
                      targetSchema: {
                        value: '@pipeline().parameters.targetSchema'
                        type: 'Expression'
                      }
                    }
                    sinkDelta: {}
                  }
                }
                staging: {}
                compute: {
                  coreCount: 8
                  computeType: 'General'
                }
                traceLevel: 'Fine'
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
        defaultValue: 'empty'
      }
      targetSchema: {
        type: 'string'
        defaultValue: 'fnd_rel'
      }
      sourceName: {
        type: 'string'
        defaultValue: 'vw_empty_hash'
      }
      sourceSchema: {
        type: 'string'
        defaultValue: 'dbo'
      }
      database: {
        type: 'string'
        defaultValue: 'satac-dai-hub'
      }
    }
    folder: {
      name: 'SCD/Transformations'
    }
    annotations: []
  }
  dependsOn: [
    '${factoryId}/datasets/ds_edaDelta'
    '${factoryId}/dataflows/MDF_SCD1_Initial_VW'
    '${factoryId}/linkedServices/ls_synapsesqlondemand_gen01'
    '${factoryId}/dataflows/MDF_SCD1_Update_VW'
  ]
}

resource factoryName_MDF_SCD2_VW 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/MDF_SCD2_VW'
  properties: {
    description: 'Creates or updates Delta files, based on SCD Type 1 logic.  Accepts parameters for the source, and target object and schema.  Creates a view of the resultant delta directory at first execution.'
    activities: [
      {
        name: 'Check If Delta Folder Exists'
        description: 'Checks to see whether a Delta file exists, based on the parsed parameters.'
        type: 'GetMetadata'
        dependsOn: []
        policy: {
          timeout: '0.00:05:00'
          retry: 1
          retryIntervalInSeconds: 30
          secureOutput: false
          secureInput: false
        }
        userProperties: []
        typeProperties: {
          dataset: {
            referenceName: 'ds_edaDelta'
            type: 'DatasetReference'
            parameters: {
              targetName: {
                value: '@pipeline().parameters.targetName'
                type: 'Expression'
              }
              targetSchema: {
                value: '@pipeline().parameters.targetSchema'
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
        }
      }
      {
        name: 'If Delta Exists Condition'
        description: 'If condition: Conditionally splits activities, based on whether delta files exist at the target.  If they do not, an initial delta hierarchy and view are created.  If they are, a merge-update is performed based on the incoming data, to the sink.'
        type: 'IfCondition'
        dependsOn: [
          {
            activity: 'Check If Delta Folder Exists'
            dependencyConditions: [
              'Succeeded'
            ]
          }
        ]
        userProperties: []
        typeProperties: {
          expression: {
            value: '@activity(\'Check If Delta Folder Exists\').output.exists'
            type: 'Expression'
          }
          ifFalseActivities: [
            {
              name: 'Data flow_Insert'
              description: 'Creates, and executes an insert to the target directory, based on the provided parameters.'
              type: 'ExecuteDataFlow'
              dependsOn: []
              policy: {
                timeout: '1.00:00:00'
                retry: 0
                retryIntervalInSeconds: 30
                secureOutput: false
                secureInput: false
              }
              userProperties: []
              typeProperties: {
                dataFlow: {
                  referenceName: 'MDF_SCD2_Initial_VW'
                  type: 'DataFlowReference'
                  parameters: {
                    sourceName: {
                      value: '\'@{pipeline().parameters.sourceName}\''
                      type: 'Expression'
                    }
                    sourceSchema: {
                      value: '\'@{pipeline().parameters.sourceSchema}\''
                      type: 'Expression'
                    }
                    targetName: {
                      value: '\'@{pipeline().parameters.targetName}\''
                      type: 'Expression'
                    }
                    targetSchema: {
                      value: '\'@{pipeline().parameters.targetSchema}\''
                      type: 'Expression'
                    }
                  }
                  datasetParameters: {
                    synSslSrcQry: {
                      targetName: {
                        value: '@pipeline().parameters.targetName'
                        type: 'Expression'
                      }
                      targetSchema: {
                        value: '@pipeline().parameters.targetSchema'
                        type: 'Expression'
                      }
                    }
                    sinkDelta: {}
                  }
                }
                staging: {}
                compute: {
                  coreCount: 8
                  computeType: 'General'
                }
                traceLevel: 'Fine'
              }
            }
            {
              name: 'CreateDeltaView'
              description: 'Creates a Delta view of the object directory, based on the provided parameters.'
              type: 'Script'
              dependsOn: [
                {
                  activity: 'Data flow_Insert'
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
              linkedServiceName: {
                referenceName: 'ls_synapsesqlondemand_gen01'
                type: 'LinkedServiceReference'
              }
              typeProperties: {
                scripts: [
                  {
                    type: 'Query'
                    text: {
                      value: '@concat(\'CREATE OR ALTER VIEW [\',pipeline().parameters.targetSchema,\'].[\',pipeline().parameters.targetName,\']\nAS\nSELECT *\nFROM\n    OPENROWSET(\n        BULK \'\'Delta/\',pipeline().parameters.targetSchema,\'/\',pipeline().parameters.targetName,\'\'\',\n        DATA_SOURCE = \'\'transformed_sataccldaidlsdev01_dfs_core_windows_net\'\',\n        FORMAT = \'\'DELTA\'\'\n    ) AS [result];\')'
                      type: 'Expression'
                    }
                  }
                ]
              }
            }
          ]
          ifTrueActivities: [
            {
              name: 'Data flow_Update'
              description: 'Execute a merge (update), based on the provided parameters.'
              type: 'ExecuteDataFlow'
              dependsOn: []
              policy: {
                timeout: '1.00:00:00'
                retry: 0
                retryIntervalInSeconds: 30
                secureOutput: false
                secureInput: false
              }
              userProperties: []
              typeProperties: {
                dataFlow: {
                  referenceName: 'MDF_SCD2_Update_VW'
                  type: 'DataFlowReference'
                  parameters: {
                    sourceName: {
                      value: '\'@{pipeline().parameters.sourceName}\''
                      type: 'Expression'
                    }
                    sourceSchema: {
                      value: '\'@{pipeline().parameters.sourceSchema}\''
                      type: 'Expression'
                    }
                    targetName: {
                      value: '\'@{pipeline().parameters.targetName}\''
                      type: 'Expression'
                    }
                    targetSchema: {
                      value: '\'@{pipeline().parameters.targetSchema}\''
                      type: 'Expression'
                    }
                  }
                  datasetParameters: {
                    SynSqlCur: {
                      targetName: {
                        value: '@pipeline().parameters.targetName'
                        type: 'Expression'
                      }
                      targetSchema: {
                        value: '@pipeline().parameters.targetSchema'
                        type: 'Expression'
                      }
                    }
                    SynSslSrcQuery: {
                      targetName: {
                        value: '@pipeline().parameters.targetName'
                        type: 'Expression'
                      }
                      targetSchema: {
                        value: '@pipeline().parameters.targetSchema'
                        type: 'Expression'
                      }
                    }
                    SinkDelta: {}
                  }
                }
                staging: {}
                compute: {
                  coreCount: 8
                  computeType: 'General'
                }
                traceLevel: 'Fine'
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
        defaultValue: 'empty_scd2'
      }
      targetSchema: {
        type: 'string'
        defaultValue: 'fnd_rel'
      }
      sourceName: {
        type: 'string'
        defaultValue: 'vw_empty_hash_scd2'
      }
      sourceSchema: {
        type: 'string'
        defaultValue: 'dbo'
      }
      database: {
        type: 'string'
        defaultValue: 'satac-dai-hub'
      }
    }
    folder: {
      name: 'SCD/Transformations'
    }
    annotations: []
  }
  dependsOn: [
    '${factoryId}/datasets/ds_edaDelta'
    '${factoryId}/dataflows/MDF_SCD2_Initial_VW'
    '${factoryId}/linkedServices/ls_synapsesqlondemand_gen01'
    '${factoryId}/dataflows/MDF_SCD2_Update_VW'
  ]
}

resource factoryName_MetadataDrivenCopy_Excel_BottomLevel 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_Excel_BottomLevel'
  properties: {
    description: 'This pipeline will copy objects from one group. The objects belonging to this group will be copied parallelly.'
    activities: [
      {
        name: 'ListObjectsFromOneGroup'
        description: 'List objects from one group and iterate each of them to downstream activities'
        type: 'ForEach'
        dependsOn: []
        userProperties: []
        typeProperties: {
          items: {
            value: '@pipeline().parameters.ObjectsPerGroupToCopy'
            type: 'Expression'
          }
          isSequential: false
          activities: [
            {
              name: 'RouteJobsBasedOnLoadingBehavior'
              description: 'Only doing full load as the files will be small keeping the same structure in case we need an incremental load.'
              type: 'Switch'
              dependsOn: [
                {
                  activity: 'GetSourceConnectionValues'
                  dependencyConditions: [
                    'Succeeded'
                  ]
                }
              ]
              userProperties: []
              typeProperties: {
                on: {
                  value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                  type: 'Expression'
                }
                cases: [
                  {
                    value: 'FullLoad'
                    activities: [
                      {
                        name: 'FullLoadOneObject'
                        description: 'Take a full snapshot on this object and copy it to the destination'
                        type: 'Copy'
                        dependsOn: [
                          {
                            activity: 'LogCSVCommence_FL'
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
                          source: {
                            type: 'ExcelSource'
                            storeSettings: {
                              type: 'FileServerReadSettings'
                              recursive: true
                              enablePartitionDiscovery: false
                            }
                          }
                          sink: {
                            type: 'DelimitedTextSink'
                            storeSettings: {
                              type: 'AzureBlobStorageWriteSettings'
                            }
                            formatSettings: {
                              type: 'DelimitedTextWriteSettings'
                              quoteAllText: true
                              fileExtension: '.txt'
                            }
                          }
                          enableStaging: false
                          translator: {
                            value: '@json(item().CopyActivitySettings).translator'
                            type: 'Expression'
                          }
                        }
                        inputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_Excel_SourceDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_worksheetName: {
                                value: '@json(item().SourceObjectSettings).sheetName'
                                type: 'Expression'
                              }
                              cw_folderName: {
                                value: '@json(item().SourceObjectSettings).filePath'
                                type: 'Expression'
                              }
                              cw_fileName: {
                                value: '@json(item().SourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              cw_ls_host: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).host'
                                type: 'Expression'
                              }
                              cw_ls_userName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).userName'
                                type: 'Expression'
                              }
                              cw_ls_passwordSecretName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).passwordSecretName'
                                type: 'Expression'
                              }
                              cw_range: {
                                value: '@json(item().SourceObjectSettings).range'
                                type: 'Expression'
                              }
                              cw_firstRowAsHeaderSource: {
                                value: '@json(item().SourceObjectSettings).firstRowAsHeaderSource'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                        outputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_FileSystem_DestinationDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_compressionCodec: {
                                value: '@json(item().SinkObjectSettings).compressionCodec'
                                type: 'Expression'
                              }
                              cw_compressionLevel: {
                                value: '@json(item().SinkObjectSettings).compressionLevel'
                                type: 'Expression'
                              }
                              cw_columnDelimiter: {
                                value: '@json(item().SinkObjectSettings).columnDelimiter'
                                type: 'Expression'
                              }
                              cw_escapeChar: {
                                value: '@json(item().SinkObjectSettings).escapeChar'
                                type: 'Expression'
                              }
                              cw_quoteChar: {
                                value: '@json(item().SinkObjectSettings).quoteChar'
                                type: 'Expression'
                              }
                              cw_firstRowAsHeader: {
                                value: '@json(item().SinkObjectSettings).firstRowAsHeader'
                                type: 'Expression'
                              }
                              cw_fileName: {
                                value: '@json(item().SinkObjectSettings).fileName'
                                type: 'Expression'
                              }
                              cw_folderPath: {
                                value: '@json(item().SinkObjectSettings).folderPath'
                                type: 'Expression'
                              }
                              cw_container: {
                                value: '@json(item().SinkObjectSettings).container'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                      }
                      {
                        name: 'FullLoadOneObject_Parquet'
                        description: 'Take a full snapshot on this object and copy it to the destination'
                        type: 'Copy'
                        dependsOn: [
                          {
                            activity: 'LogParquetCommence_FL'
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
                        userProperties: [
                          {
                            name: 'Source'
                            value: '@{json(item().ParquetSourceObjectSettings).fileSystem}/@{json(item().ParquetSourceObjectSettings).folderPath}/@{json(item().ParquetSourceObjectSettings).fileName}'
                          }
                          {
                            name: 'Destination'
                            value: '@{json(item().ParquetSinkObjectSettings).fileSystem}/@{json(item().ParquetSinkObjectSettings).folderPath}/@{json(item().ParquetSinkObjectSettings).fileName}'
                          }
                        ]
                        typeProperties: {
                          source: {
                            type: 'DelimitedTextSource'
                            additionalColumns: [
                              {
                                name: 'loadDate'
                                value: {
                                  value: '@formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyyMMdd\')'
                                  type: 'Expression'
                                }
                              }
                            ]
                            storeSettings: {
                              type: 'AzureBlobStorageReadSettings'
                              recursive: true
                              enablePartitionDiscovery: false
                            }
                            formatSettings: {
                              type: 'DelimitedTextReadSettings'
                            }
                          }
                          sink: {
                            type: 'ParquetSink'
                            storeSettings: {
                              type: 'AzureBlobStorageWriteSettings'
                              copyBehavior: {
                                value: '@json(item().ParquetCopySinkSettings).copyBehavior'
                                type: 'Expression'
                              }
                            }
                            formatSettings: {
                              type: 'ParquetWriteSettings'
                            }
                          }
                          enableStaging: false
                          validateDataConsistency: true
                          translator: {
                            value: '@json(item().ParquetCopyActivitySettings).translator'
                            type: 'Expression'
                          }
                        }
                        inputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_FileSystem_ParquetSourceDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_columnDelimiter: {
                                value: '@json(item().ParquetSourceObjectSettings).columnDelimiter'
                                type: 'Expression'
                              }
                              cw_escapeChar: {
                                value: '@json(item().ParquetSourceObjectSettings).escapeChar'
                                type: 'Expression'
                              }
                              cw_quoteChar: {
                                value: '@json(item().ParquetSourceObjectSettings).quoteChar'
                                type: 'Expression'
                              }
                              cw_firstRowAsHeader: {
                                value: '@json(item().ParquetSourceObjectSettings).firstRowAsHeader'
                                type: 'Expression'
                              }
                              cw_fileName: {
                                value: '@json(item().ParquetSourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              cw_folderPath: {
                                value: '@json(item().ParquetSourceObjectSettings).folderPath'
                                type: 'Expression'
                              }
                              cw_fileSystem: {
                                value: '@json(item().ParquetSourceObjectSettings).fileSystem'
                                type: 'Expression'
                              }
                              cw_container: {
                                value: '@json(item().ParquetSourceObjectSettings).container'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                        outputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_FileSystem_ParquetDestinationDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_compressionCodec: {
                                value: '@json(item().ParquetSinkObjectSettings).compressionCodec'
                                type: 'Expression'
                              }
                              cw_columnDelimiter: {
                                value: '@json(item().ParquetSinkObjectSettings).columnDelimiter'
                                type: 'Expression'
                              }
                              cw_escapeChar: {
                                value: '@json(item().ParquetSinkObjectSettings).escapeChar'
                                type: 'Expression'
                              }
                              cw_quoteChar: {
                                value: '@json(item().ParquetSinkObjectSettings).quoteChar'
                                type: 'Expression'
                              }
                              cw_fileName: {
                                value: '@json(item().ParquetSinkObjectSettings).fileName'
                                type: 'Expression'
                              }
                              cw_folderPath: {
                                value: '@json(item().ParquetSinkObjectSettings).folderPath'
                                type: 'Expression'
                              }
                              cw_fileSystem: {
                                value: '@json(item().ParquetSinkObjectSettings).fileSystem'
                                type: 'Expression'
                              }
                              cw_container: {
                                value: '@json(item().ParquetSinkObjectSettings).container'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                      }
                      {
                        name: 'LogCSVCommence_FL'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'csv copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'commenced'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).sheetName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogCSVSuccess_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'FullLoadOneObject'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'csv copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'succeeded'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).sheetName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: {
                                value: '@activity(\'FullLoadOneObject\').output?.rowsCopied'
                                type: 'Expression'
                              }
                              type: 'Int64'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogParquetCommence_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'LogCSVSuccess_FL'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'parquet copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'commenced'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).sheetName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogCSVFail_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'FullLoadOneObject'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'csv copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: {
                                value: '@activity(\'FullLoadOneObject\').error?.message'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'failed'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).sheetName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'Fail_LogCSVFail_FL'
                        type: 'Fail'
                        dependsOn: [
                          {
                            activity: 'LogCSVFail_FL'
                            dependencyConditions: [
                              'Completed'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          message: {
                            value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                            type: 'Expression'
                          }
                          errorCode: '3204'
                        }
                      }
                      {
                        name: 'LogParquetSuccess_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'FullLoadOneObject_Parquet'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'parquet copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'succeeded'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).sheetName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: {
                                value: '@activity(\'FullLoadOneObject_Parquet\').output?.rowsCopied'
                                type: 'Expression'
                              }
                              type: 'Int64'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogParquetFail_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'FullLoadOneObject_Parquet'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'parquet copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: {
                                value: '@activity(\'FullLoadOneObject_Parquet\').error?.message'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'failed'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).sheetName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'Fail_LogParquetFail_FL'
                        type: 'Fail'
                        dependsOn: [
                          {
                            activity: 'LogParquetFail_FL'
                            dependencyConditions: [
                              'Completed'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          message: {
                            value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                            type: 'Expression'
                          }
                          errorCode: '3204'
                        }
                      }
                    ]
                  }
                ]
                defaultActivities: [
                  {
                    name: 'DefaultOneObject'
                    description: 'Take a full snapshot on this object and copy it to the destination'
                    type: 'Copy'
                    dependsOn: [
                      {
                        activity: 'LogCSVCommence'
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
                      source: {
                        type: 'ExcelSource'
                        storeSettings: {
                          type: 'FileServerReadSettings'
                          recursive: true
                          enablePartitionDiscovery: false
                        }
                      }
                      sink: {
                        type: 'DelimitedTextSink'
                        storeSettings: {
                          type: 'AzureBlobStorageWriteSettings'
                        }
                        formatSettings: {
                          type: 'DelimitedTextWriteSettings'
                          quoteAllText: true
                          fileExtension: '.txt'
                        }
                      }
                      enableStaging: false
                      translator: {
                        value: '@json(item().CopyActivitySettings).translator'
                        type: 'Expression'
                      }
                    }
                    inputs: [
                      {
                        referenceName: 'MetadataDrivenCopy_Excel_SourceDS'
                        type: 'DatasetReference'
                        parameters: {
                          cw_worksheetName: {
                            value: '@json(item().SourceObjectSettings).sheetName'
                            type: 'Expression'
                          }
                          cw_folderName: {
                            value: '@json(item().SourceObjectSettings).filePath'
                            type: 'Expression'
                          }
                          cw_fileName: {
                            value: '@json(item().SourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          cw_ls_host: {
                            value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).host'
                            type: 'Expression'
                          }
                          cw_ls_userName: {
                            value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).userName'
                            type: 'Expression'
                          }
                          cw_ls_passwordSecretName: {
                            value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).passwordSecretName'
                            type: 'Expression'
                          }
                          cw_range: {
                            value: '@json(item().SourceObjectSettings).range'
                            type: 'Expression'
                          }
                          cw_firstRowAsHeaderSource: {
                            value: '@json(item().SourceObjectSettings).firstRowAsHeaderSource'
                            type: 'Expression'
                          }
                        }
                      }
                    ]
                    outputs: [
                      {
                        referenceName: 'MetadataDrivenCopy_FileSystem_DestinationDS'
                        type: 'DatasetReference'
                        parameters: {
                          cw_compressionCodec: {
                            value: '@json(item().SinkObjectSettings).compressionCodec'
                            type: 'Expression'
                          }
                          cw_compressionLevel: {
                            value: '@json(item().SinkObjectSettings).compressionLevel'
                            type: 'Expression'
                          }
                          cw_columnDelimiter: {
                            value: '@json(item().SinkObjectSettings).columnDelimiter'
                            type: 'Expression'
                          }
                          cw_escapeChar: {
                            value: '@json(item().SinkObjectSettings).escapeChar'
                            type: 'Expression'
                          }
                          cw_quoteChar: {
                            value: '@json(item().SinkObjectSettings).quoteChar'
                            type: 'Expression'
                          }
                          cw_firstRowAsHeader: {
                            value: '@json(item().SinkObjectSettings).firstRowAsHeader'
                            type: 'Expression'
                          }
                          cw_fileName: {
                            value: '@json(item().SinkObjectSettings).fileName'
                            type: 'Expression'
                          }
                          cw_folderPath: {
                            value: '@json(item().SinkObjectSettings).folderPath'
                            type: 'Expression'
                          }
                          cw_container: {
                            value: '@json(item().SinkObjectSettings).container'
                            type: 'Expression'
                          }
                        }
                      }
                    ]
                  }
                  {
                    name: 'DefaultOneObject_Parquet'
                    description: 'Take a full snapshot on this object and copy it to the destination'
                    type: 'Copy'
                    dependsOn: [
                      {
                        activity: 'LogParquetCommence_FL_copy1'
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
                    userProperties: [
                      {
                        name: 'Source'
                        value: '@{json(item().ParquetSourceObjectSettings).fileSystem}/@{json(item().ParquetSourceObjectSettings).folderPath}/@{json(item().ParquetSourceObjectSettings).fileName}'
                      }
                      {
                        name: 'Destination'
                        value: '@{json(item().ParquetSinkObjectSettings).fileSystem}/@{json(item().ParquetSinkObjectSettings).folderPath}/@{json(item().ParquetSinkObjectSettings).fileName}'
                      }
                    ]
                    typeProperties: {
                      source: {
                        type: 'DelimitedTextSource'
                        additionalColumns: [
                          {
                            name: 'loadDate'
                            value: {
                              value: '@formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyyMMdd\')'
                              type: 'Expression'
                            }
                          }
                        ]
                        storeSettings: {
                          type: 'AzureBlobStorageReadSettings'
                          recursive: true
                          enablePartitionDiscovery: false
                        }
                        formatSettings: {
                          type: 'DelimitedTextReadSettings'
                        }
                      }
                      sink: {
                        type: 'ParquetSink'
                        storeSettings: {
                          type: 'AzureBlobStorageWriteSettings'
                          copyBehavior: {
                            value: '@json(item().ParquetCopySinkSettings).copyBehavior'
                            type: 'Expression'
                          }
                        }
                        formatSettings: {
                          type: 'ParquetWriteSettings'
                        }
                      }
                      enableStaging: false
                      validateDataConsistency: true
                      translator: {
                        value: '@json(item().ParquetCopyActivitySettings).translator'
                        type: 'Expression'
                      }
                    }
                    inputs: [
                      {
                        referenceName: 'MetadataDrivenCopy_FileSystem_ParquetSourceDS'
                        type: 'DatasetReference'
                        parameters: {
                          cw_columnDelimiter: {
                            value: '@json(item().ParquetSourceObjectSettings).columnDelimiter'
                            type: 'Expression'
                          }
                          cw_escapeChar: {
                            value: '@json(item().ParquetSourceObjectSettings).escapeChar'
                            type: 'Expression'
                          }
                          cw_quoteChar: {
                            value: '@json(item().ParquetSourceObjectSettings).quoteChar'
                            type: 'Expression'
                          }
                          cw_firstRowAsHeader: {
                            value: '@json(item().ParquetSourceObjectSettings).firstRowAsHeader'
                            type: 'Expression'
                          }
                          cw_fileName: {
                            value: '@json(item().ParquetSourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          cw_folderPath: {
                            value: '@json(item().ParquetSourceObjectSettings).folderPath'
                            type: 'Expression'
                          }
                          cw_fileSystem: {
                            value: '@json(item().ParquetSourceObjectSettings).fileSystem'
                            type: 'Expression'
                          }
                          cw_container: {
                            value: '@json(item().ParquetSourceObjectSettings).container'
                            type: 'Expression'
                          }
                        }
                      }
                    ]
                    outputs: [
                      {
                        referenceName: 'MetadataDrivenCopy_FileSystem_ParquetDestinationDS'
                        type: 'DatasetReference'
                        parameters: {
                          cw_compressionCodec: {
                            value: '@json(item().ParquetSinkObjectSettings).compressionCodec'
                            type: 'Expression'
                          }
                          cw_columnDelimiter: {
                            value: '@json(item().ParquetSinkObjectSettings).columnDelimiter'
                            type: 'Expression'
                          }
                          cw_escapeChar: {
                            value: '@json(item().ParquetSinkObjectSettings).escapeChar'
                            type: 'Expression'
                          }
                          cw_quoteChar: {
                            value: '@json(item().ParquetSinkObjectSettings).quoteChar'
                            type: 'Expression'
                          }
                          cw_fileName: {
                            value: '@json(item().ParquetSinkObjectSettings).fileName'
                            type: 'Expression'
                          }
                          cw_folderPath: {
                            value: '@json(item().ParquetSinkObjectSettings).folderPath'
                            type: 'Expression'
                          }
                          cw_fileSystem: {
                            value: '@json(item().ParquetSinkObjectSettings).fileSystem'
                            type: 'Expression'
                          }
                          cw_container: {
                            value: '@json(item().ParquetSinkObjectSettings).container'
                            type: 'Expression'
                          }
                        }
                      }
                    ]
                  }
                  {
                    name: 'LogCSVCommence'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'csv copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: null
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'commenced'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).sheetName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'LogCSVSuccess'
                    type: 'SqlServerStoredProcedure'
                    dependsOn: [
                      {
                        activity: 'DefaultOneObject'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'csv copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: null
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'succeeded'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).sheetName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                        rowsCopied: {
                          value: {
                            value: '@activity(\'DefaultOneObject\').output?.rowsCopied'
                            type: 'Expression'
                          }
                          type: 'Int64'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'LogParquetCommence_FL_copy1'
                    type: 'SqlServerStoredProcedure'
                    dependsOn: [
                      {
                        activity: 'LogCSVSuccess'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'parquet copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: null
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'commenced'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).sheetName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'LogCSVFail'
                    type: 'SqlServerStoredProcedure'
                    dependsOn: [
                      {
                        activity: 'DefaultOneObject'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'csv copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: {
                            value: '@activity(\'DefaultOneObject\').error?.message'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'failed'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).sheetName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'Fail_LogCSVFail'
                    type: 'Fail'
                    dependsOn: [
                      {
                        activity: 'LogCSVFail'
                        dependencyConditions: [
                          'Completed'
                        ]
                      }
                    ]
                    userProperties: []
                    typeProperties: {
                      message: {
                        value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                        type: 'Expression'
                      }
                      errorCode: '3204'
                    }
                  }
                  {
                    name: 'LogParquetSuccess'
                    type: 'SqlServerStoredProcedure'
                    dependsOn: [
                      {
                        activity: 'DefaultOneObject_Parquet'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'parquet copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: null
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'succeeded'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).sheetName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                        rowsCopied: {
                          value: {
                            value: '@activity(\'DefaultOneObject_Parquet\').output?.rowsCopied'
                            type: 'Expression'
                          }
                          type: 'Int64'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'LogParquetFail'
                    type: 'SqlServerStoredProcedure'
                    dependsOn: [
                      {
                        activity: 'DefaultOneObject_Parquet'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'parquet copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: {
                            value: '@activity(\'DefaultOneObject_Parquet\').error?.message'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'failed'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).sheetName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'Fail_LogParquetFail'
                    type: 'Fail'
                    dependsOn: [
                      {
                        activity: 'LogParquetFail'
                        dependencyConditions: [
                          'Completed'
                        ]
                      }
                    ]
                    userProperties: []
                    typeProperties: {
                      message: {
                        value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                        type: 'Expression'
                      }
                      errorCode: '3204'
                    }
                  }
                ]
              }
            }
            {
              name: 'GetSourceConnectionValues'
              type: 'Lookup'
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
                source: {
                  type: 'AzureSqlSource'
                  sqlReaderQuery: {
                    value: 'select ConnectionSettings from @{pipeline().parameters.ConnectionControlTableName} where Name = \'@{item().SourceConnectionSettingsName} \' AND Id = @{item().Id}'
                    type: 'Expression'
                  }
                  partitionOption: 'None'
                }
                dataset: {
                  referenceName: 'MetadataDrivenCopy_SQL_ControlDS'
                  type: 'DatasetReference'
                  parameters: {}
                }
                firstRowOnly: false
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
      ObjectsPerGroupToCopy: {
        type: 'Array'
      }
      ConnectionControlTableName: {
        type: 'String'
      }
      TriggerName: {
        type: 'string'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_FileSystem'
    }
    annotations: []
    lastPublishTime: '2022-05-23T04:36:04Z'
  }
  dependsOn: [
    '${factoryId}/datasets/MetadataDrivenCopy_SQL_ControlDS'
    '${factoryId}/datasets/MetadataDrivenCopy_Excel_SourceDS'
    '${factoryId}/datasets/MetadataDrivenCopy_FileSystem_DestinationDS'
    '${factoryId}/datasets/MetadataDrivenCopy_FileSystem_ParquetSourceDS'
    '${factoryId}/datasets/MetadataDrivenCopy_FileSystem_ParquetDestinationDS'
    '${factoryId}/linkedServices/ls_azsqldb_metadatacontroldb'
  ]
}

resource factoryName_MetadataDrivenCopy_Excel_MiddleLevel 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_Excel_MiddleLevel'
  properties: {
    description: 'This pipeline will copy one batch of objects. The objects belonging to this batch will be copied parallelly.'
    activities: [
      {
        name: 'DivideOneBatchIntoMultipleGroups'
        description: 'Divide objects from single batch into multiple sub parallel groups to avoid reaching the output limit of lookup activity.'
        type: 'ForEach'
        dependsOn: []
        userProperties: []
        typeProperties: {
          items: {
            value: '@range(0, add(div(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity),\n                    if(equals(mod(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity), 0), 0, 1)))'
            type: 'Expression'
          }
          isSequential: false
          batchCount: 50
          activities: [
            {
              name: 'GetObjectsPerGroupToCopy'
              description: 'Get objects (tables etc.) from control table required to be copied in this group. The order of objects to be copied following the TaskId in control table (ORDER BY [TaskId] DESC).'
              type: 'Lookup'
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
                source: {
                  type: 'AzureSqlSource'
                  sqlReaderQuery: {
                    value: 'WITH OrderedControlTable AS (\n                             SELECT *, ROW_NUMBER() OVER (ORDER BY [TaskId], [Id] DESC) AS RowNumber\n                             FROM @{pipeline().parameters.MainControlTableName}\n                             where TopLevelPipelineName = \'@{pipeline().parameters.TopLevelPipelineName}\'\n                             and TriggerName like \'%@{pipeline().parameters.TriggerName}%\' and CopyEnabled = 1)\n                             SELECT * FROM OrderedControlTable WHERE RowNumber BETWEEN @{add(mul(int(item()),pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity),\n                             add(mul(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.CurrentSequentialNumberOfBatch), 1))}\n                             AND @{min(add(mul(int(item()), pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity), add(mul(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.CurrentSequentialNumberOfBatch),\n                             pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity)),\n                            mul(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, add(pipeline().parameters.CurrentSequentialNumberOfBatch,1)), pipeline().parameters.SumOfObjectsToCopy)}'
                    type: 'Expression'
                  }
                  partitionOption: 'None'
                }
                dataset: {
                  referenceName: 'MetadataDrivenCopy_Oracle_ControlDS'
                  type: 'DatasetReference'
                  parameters: {}
                }
                firstRowOnly: false
              }
            }
            {
              name: 'CopyObjectsInOneGroup'
              description: 'Execute another pipeline to copy objects from one group. The objects belonging to this group will be copied parallelly.'
              type: 'ExecutePipeline'
              dependsOn: [
                {
                  activity: 'GetObjectsPerGroupToCopy'
                  dependencyConditions: [
                    'Succeeded'
                  ]
                }
              ]
              userProperties: []
              typeProperties: {
                pipeline: {
                  referenceName: 'MetadataDrivenCopy_Excel_BottomLevel'
                  type: 'PipelineReference'
                }
                waitOnCompletion: true
                parameters: {
                  ObjectsPerGroupToCopy: {
                    value: '@activity(\'GetObjectsPerGroupToCopy\').output.value'
                    type: 'Expression'
                  }
                  ConnectionControlTableName: {
                    value: '@pipeline().parameters.ConnectionControlTableName'
                    type: 'Expression'
                  }
                  TriggerName: {
                    value: '@pipeline().parameters.TriggerName'
                    type: 'Expression'
                  }
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
      MaxNumberOfObjectsReturnedFromLookupActivity: {
        type: 'Int'
      }
      TopLevelPipelineName: {
        type: 'String'
      }
      TriggerName: {
        type: 'String'
      }
      CurrentSequentialNumberOfBatch: {
        type: 'Int'
      }
      SumOfObjectsToCopy: {
        type: 'Int'
      }
      SumOfObjectsToCopyForCurrentBatch: {
        type: 'Int'
      }
      MainControlTableName: {
        type: 'String'
      }
      ConnectionControlTableName: {
        type: 'String'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_FileSystem'
    }
    annotations: []
    lastPublishTime: '2022-05-11T07:03:14Z'
  }
  dependsOn: [
    '${factoryId}/datasets/MetadataDrivenCopy_Oracle_ControlDS'
    '${factoryId}/pipelines/MetadataDrivenCopy_Excel_BottomLevel'
  ]
}

resource factoryName_MetadataDrivenCopy_Excel_TopLevel 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_Excel_TopLevel'
  properties: {
    description: 'This pipeline will count the total number of objects (tables etc.) required to be copied in this run, come up with the number of sequential batches based on the max allowed concurrent copy task, and then execute another pipeline to copy different batches sequentially.'
    activities: [
      {
        name: 'GetSumOfObjectsToCopy'
        description: 'Count the total number of objects (tables etc.) required to be copied in this run.'
        type: 'Lookup'
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
          source: {
            type: 'AzureSqlSource'
            sqlReaderQuery: {
              value: 'SELECT count(*) as count FROM @{pipeline().parameters.MainControlTableName} where TopLevelPipelineName=\'@{pipeline().Pipeline}\' and TriggerName like \'%@{pipeline().TriggerName}%\' and CopyEnabled = 1'
              type: 'Expression'
            }
            partitionOption: 'None'
          }
          dataset: {
            referenceName: 'MetadataDrivenCopy_Oracle_ControlDS'
            type: 'DatasetReference'
            parameters: {}
          }
        }
      }
      {
        name: 'CopyBatchesOfObjectsSequentially'
        description: 'Come up with the number of sequential batches based on the max allowed concurrent copy tasks, and then execute another pipeline to copy different batches sequentially.'
        type: 'ForEach'
        dependsOn: [
          {
            activity: 'GetSumOfObjectsToCopy'
            dependencyConditions: [
              'Succeeded'
            ]
          }
        ]
        userProperties: []
        typeProperties: {
          items: {
            value: '@range(0, add(div(activity(\'GetSumOfObjectsToCopy\').output.firstRow.count,\n                    pipeline().parameters.MaxNumberOfConcurrentTasks),\n                    if(equals(mod(activity(\'GetSumOfObjectsToCopy\').output.firstRow.count,\n                    pipeline().parameters.MaxNumberOfConcurrentTasks), 0), 0, 1)))'
            type: 'Expression'
          }
          isSequential: true
          activities: [
            {
              name: 'CopyObjectsInOneBatch'
              description: 'Execute another pipeline to copy one batch of objects. The objects belonging to this batch will be copied parallelly.'
              type: 'ExecutePipeline'
              dependsOn: []
              userProperties: []
              typeProperties: {
                pipeline: {
                  referenceName: 'MetadataDrivenCopy_Excel_MiddleLevel'
                  type: 'PipelineReference'
                }
                waitOnCompletion: true
                parameters: {
                  MaxNumberOfObjectsReturnedFromLookupActivity: {
                    value: '@pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity'
                    type: 'Expression'
                  }
                  TopLevelPipelineName: {
                    value: '@{pipeline().Pipeline}'
                    type: 'Expression'
                  }
                  TriggerName: {
                    value: '@{pipeline().TriggerName}'
                    type: 'Expression'
                  }
                  CurrentSequentialNumberOfBatch: {
                    value: '@item()'
                    type: 'Expression'
                  }
                  SumOfObjectsToCopy: {
                    value: '@activity(\'GetSumOfObjectsToCopy\').output.firstRow.count'
                    type: 'Expression'
                  }
                  SumOfObjectsToCopyForCurrentBatch: {
                    value: '@min(pipeline().parameters.MaxNumberOfConcurrentTasks, activity(\'GetSumOfObjectsToCopy\').output.firstRow.count)'
                    type: 'Expression'
                  }
                  MainControlTableName: {
                    value: '@pipeline().parameters.MainControlTableName'
                    type: 'Expression'
                  }
                  ConnectionControlTableName: {
                    value: '@pipeline().parameters.ConnectionControlTableName'
                    type: 'Expression'
                  }
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
      MaxNumberOfObjectsReturnedFromLookupActivity: {
        type: 'Int'
        defaultValue: 5000
      }
      MaxNumberOfConcurrentTasks: {
        type: 'Int'
        defaultValue: 16
      }
      MainControlTableName: {
        type: 'String'
        defaultValue: 'config.FileSystemControlTable'
      }
      ConnectionControlTableName: {
        type: 'String'
        defaultValue: 'config.FileSystemConnectionControlTable'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_FileSystem'
    }
    annotations: [
      'MetadataDrivenSolution'
    ]
    lastPublishTime: '2022-05-12T23:34:19Z'
  }
  dependsOn: [
    '${factoryId}/datasets/MetadataDrivenCopy_Oracle_ControlDS'
    '${factoryId}/pipelines/MetadataDrivenCopy_Excel_MiddleLevel'
  ]
}

resource factoryName_MetadataDrivenCopy_FlatFile_BottomLevel 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_FlatFile_BottomLevel'
  properties: {
    description: 'This pipeline will copy objects from one group. The objects belonging to this group will be copied parallelly.'
    activities: [
      {
        name: 'ListObjectsFromOneGroup'
        description: 'List objects from one group and iterate each of them to downstream activities'
        type: 'ForEach'
        dependsOn: []
        userProperties: []
        typeProperties: {
          items: {
            value: '@pipeline().parameters.ObjectsPerGroupToCopy'
            type: 'Expression'
          }
          isSequential: false
          activities: [
            {
              name: 'RouteJobsBasedOnLoadingBehavior'
              description: 'Only doing full load as the files will be small keeping the same structure in case we need an incremental load.'
              type: 'Switch'
              dependsOn: [
                {
                  activity: 'GetSourceConnectionValues'
                  dependencyConditions: [
                    'Succeeded'
                  ]
                }
              ]
              userProperties: []
              typeProperties: {
                on: {
                  value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                  type: 'Expression'
                }
                cases: [
                  {
                    value: 'FullLoad'
                    activities: [
                      {
                        name: 'FullLoadOneObject_Parquet'
                        description: 'Take a full snapshot on this object and copy it to the destination'
                        type: 'Copy'
                        dependsOn: [
                          {
                            activity: 'LogParquetCommence_FL'
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
                        userProperties: [
                          {
                            name: 'Source'
                            value: '@{json(item().ParquetSourceObjectSettings).fileSystem}/@{json(item().ParquetSourceObjectSettings).folderPath}/@{json(item().ParquetSourceObjectSettings).fileName}'
                          }
                          {
                            name: 'Destination'
                            value: '@{json(item().ParquetSinkObjectSettings).fileSystem}/@{json(item().ParquetSinkObjectSettings).folderPath}/@{json(item().ParquetSinkObjectSettings).fileName}'
                          }
                        ]
                        typeProperties: {
                          source: {
                            type: 'DelimitedTextSource'
                            additionalColumns: [
                              {
                                name: 'loadDate'
                                value: {
                                  value: '@formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyyMMdd\')'
                                  type: 'Expression'
                                }
                              }
                            ]
                            storeSettings: {
                              type: 'AzureBlobStorageReadSettings'
                              recursive: true
                              enablePartitionDiscovery: false
                            }
                            formatSettings: {
                              type: 'DelimitedTextReadSettings'
                            }
                          }
                          sink: {
                            type: 'ParquetSink'
                            storeSettings: {
                              type: 'AzureBlobStorageWriteSettings'
                              copyBehavior: {
                                value: '@json(item().ParquetCopySinkSettings).copyBehavior'
                                type: 'Expression'
                              }
                            }
                            formatSettings: {
                              type: 'ParquetWriteSettings'
                            }
                          }
                          enableStaging: false
                          validateDataConsistency: true
                          translator: {
                            value: '@json(item().ParquetCopyActivitySettings).translator'
                            type: 'Expression'
                          }
                        }
                        inputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_FileSystem_ParquetSourceDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_columnDelimiter: {
                                value: '@json(item().ParquetSourceObjectSettings).columnDelimiter'
                                type: 'Expression'
                              }
                              cw_escapeChar: {
                                value: '@json(item().ParquetSourceObjectSettings).escapeChar'
                                type: 'Expression'
                              }
                              cw_quoteChar: {
                                value: '@json(item().ParquetSourceObjectSettings).quoteChar'
                                type: 'Expression'
                              }
                              cw_firstRowAsHeader: {
                                value: '@json(item().ParquetSourceObjectSettings).firstRowAsHeader'
                                type: 'Expression'
                              }
                              cw_fileName: {
                                value: '@json(item().ParquetSourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              cw_folderPath: {
                                value: '@json(item().ParquetSourceObjectSettings).folderPath'
                                type: 'Expression'
                              }
                              cw_fileSystem: {
                                value: '@json(item().ParquetSourceObjectSettings).fileSystem'
                                type: 'Expression'
                              }
                              cw_container: {
                                value: '@json(item().ParquetSourceObjectSettings).container'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                        outputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_FileSystem_ParquetDestinationDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_compressionCodec: {
                                value: '@json(item().ParquetSinkObjectSettings).compressionCodec'
                                type: 'Expression'
                              }
                              cw_columnDelimiter: {
                                value: '@json(item().ParquetSinkObjectSettings).columnDelimiter'
                                type: 'Expression'
                              }
                              cw_escapeChar: {
                                value: '@json(item().ParquetSinkObjectSettings).escapeChar'
                                type: 'Expression'
                              }
                              cw_quoteChar: {
                                value: '@json(item().ParquetSinkObjectSettings).quoteChar'
                                type: 'Expression'
                              }
                              cw_fileName: {
                                value: '@json(item().ParquetSinkObjectSettings).fileName'
                                type: 'Expression'
                              }
                              cw_folderPath: {
                                value: '@json(item().ParquetSinkObjectSettings).folderPath'
                                type: 'Expression'
                              }
                              cw_fileSystem: {
                                value: '@json(item().ParquetSinkObjectSettings).fileSystem'
                                type: 'Expression'
                              }
                              cw_container: {
                                value: '@json(item().ParquetSinkObjectSettings).container'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                      }
                      {
                        name: 'LogCSVCommence_FL'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'csv copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'commenced'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).sheetName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogCSVSuccess_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'FullLoadOneObject'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'csv copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'succeeded'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).sheetName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: {
                                value: '@activity(\'FullLoadOneObject\').output?.rowsCopied'
                                type: 'Expression'
                              }
                              type: 'Int64'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogParquetCommence_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'LogCSVSuccess_FL'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'parquet copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'commenced'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).sheetName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogCSVFail_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'FullLoadOneObject'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'csv copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: {
                                value: '@activity(\'FullLoadOneObject\').error?.message'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'failed'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).sheetName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'Fail_LogCSVFail_FL'
                        type: 'Fail'
                        dependsOn: [
                          {
                            activity: 'LogCSVFail_FL'
                            dependencyConditions: [
                              'Completed'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          message: {
                            value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                            type: 'Expression'
                          }
                          errorCode: '3204'
                        }
                      }
                      {
                        name: 'LogParquetSuccess_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'FullLoadOneObject_Parquet'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'parquet copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'succeeded'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).sheetName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: {
                                value: '@activity(\'FullLoadOneObject_Parquet\').output?.rowsCopied'
                                type: 'Expression'
                              }
                              type: 'Int64'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogParquetFail_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'FullLoadOneObject_Parquet'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'parquet copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: {
                                value: '@activity(\'FullLoadOneObject_Parquet\').error?.message'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'failed'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).sheetName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'Fail_LogParquetFail_FL'
                        type: 'Fail'
                        dependsOn: [
                          {
                            activity: 'LogParquetFail_FL'
                            dependencyConditions: [
                              'Completed'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          message: {
                            value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                            type: 'Expression'
                          }
                          errorCode: '3204'
                        }
                      }
                      {
                        name: 'FullLoadOneObject'
                        description: 'Take a full snapshot on this object and copy it to the destination'
                        type: 'Copy'
                        dependsOn: [
                          {
                            activity: 'LogCSVCommence_FL'
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
                          source: {
                            type: 'DelimitedTextSource'
                            storeSettings: {
                              type: 'FileServerReadSettings'
                              recursive: true
                              enablePartitionDiscovery: false
                            }
                            formatSettings: {
                              type: 'DelimitedTextReadSettings'
                            }
                          }
                          sink: {
                            type: 'DelimitedTextSink'
                            storeSettings: {
                              type: 'AzureBlobStorageWriteSettings'
                            }
                            formatSettings: {
                              type: 'DelimitedTextWriteSettings'
                              quoteAllText: true
                              fileExtension: '.txt'
                            }
                          }
                          enableStaging: false
                          translator: {
                            value: '@json(item().CopyActivitySettings).translator'
                            type: 'Expression'
                          }
                        }
                        inputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_FileSystem_SourceDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_worksheetName: {
                                value: '@json(item().SourceObjectSettings).sheetName'
                                type: 'Expression'
                              }
                              cw_folderName: {
                                value: '@json(item().SourceObjectSettings).filePath'
                                type: 'Expression'
                              }
                              cw_fileName: {
                                value: '@json(item().SourceObjectSettings).fileName'
                                type: 'Expression'
                              }
                              cw_ls_host: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).host'
                                type: 'Expression'
                              }
                              cw_ls_userName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).userName'
                                type: 'Expression'
                              }
                              cw_ls_passwordSecretName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).passwordSecretName'
                                type: 'Expression'
                              }
                              cw_range: {
                                value: '@json(item().SourceObjectSettings).range'
                                type: 'Expression'
                              }
                              cw_columnDelimiter: {
                                value: '@json(item().SinkObjectSettings).columnDelimiter'
                                type: 'Expression'
                              }
                              cw_escapeChar: {
                                value: '@json(item().SinkObjectSettings).escapeChar'
                                type: 'Expression'
                              }
                              cw_quoteChar: {
                                value: '@json(item().SinkObjectSettings).quoteChar'
                                type: 'Expression'
                              }
                              cw_firstRowAsHeaderSource: {
                                value: '@json(item().SourceObjectSettings).firstRowAsHeaderSource'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                        outputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_FileSystem_DestinationDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_compressionCodec: {
                                value: '@json(item().SinkObjectSettings).compressionCodec'
                                type: 'Expression'
                              }
                              cw_compressionLevel: {
                                value: '@json(item().SinkObjectSettings).compressionLevel'
                                type: 'Expression'
                              }
                              cw_columnDelimiter: {
                                value: '@json(item().SinkObjectSettings).columnDelimiter'
                                type: 'Expression'
                              }
                              cw_escapeChar: {
                                value: '@json(item().SinkObjectSettings).escapeChar'
                                type: 'Expression'
                              }
                              cw_quoteChar: {
                                value: '@json(item().SinkObjectSettings).quoteChar'
                                type: 'Expression'
                              }
                              cw_firstRowAsHeader: {
                                value: '@json(item().SinkObjectSettings).firstRowAsHeader'
                                type: 'Expression'
                              }
                              cw_fileName: {
                                value: '@json(item().SinkObjectSettings).fileName'
                                type: 'Expression'
                              }
                              cw_folderPath: {
                                value: '@json(item().SinkObjectSettings).folderPath'
                                type: 'Expression'
                              }
                              cw_container: {
                                value: '@json(item().SinkObjectSettings).container'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                      }
                    ]
                  }
                ]
                defaultActivities: [
                  {
                    name: 'DefaultOneObject'
                    description: 'Take a full snapshot on this object and copy it to the destination'
                    type: 'Copy'
                    dependsOn: [
                      {
                        activity: 'LogCSVCommence'
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
                      source: {
                        type: 'DelimitedTextSource'
                        storeSettings: {
                          type: 'FileServerReadSettings'
                          recursive: true
                          enablePartitionDiscovery: false
                        }
                        formatSettings: {
                          type: 'DelimitedTextReadSettings'
                        }
                      }
                      sink: {
                        type: 'DelimitedTextSink'
                        storeSettings: {
                          type: 'AzureBlobStorageWriteSettings'
                        }
                        formatSettings: {
                          type: 'DelimitedTextWriteSettings'
                          quoteAllText: true
                          fileExtension: '.txt'
                        }
                      }
                      enableStaging: false
                      translator: {
                        value: '@json(item().CopyActivitySettings).translator'
                        type: 'Expression'
                      }
                    }
                    inputs: [
                      {
                        referenceName: 'MetadataDrivenCopy_FileSystem_SourceDS'
                        type: 'DatasetReference'
                        parameters: {
                          cw_worksheetName: {
                            value: '@json(item().SourceObjectSettings).sheetName'
                            type: 'Expression'
                          }
                          cw_folderName: {
                            value: '@json(item().SourceObjectSettings).filePath'
                            type: 'Expression'
                          }
                          cw_fileName: {
                            value: '@json(item().SourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          cw_ls_host: {
                            value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).host'
                            type: 'Expression'
                          }
                          cw_ls_userName: {
                            value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).userName'
                            type: 'Expression'
                          }
                          cw_ls_passwordSecretName: {
                            value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).passwordSecretName'
                            type: 'Expression'
                          }
                          cw_range: {
                            value: '@json(item().SourceObjectSettings).range'
                            type: 'Expression'
                          }
                          cw_columnDelimiter: {
                            value: '@json(item().SinkObjectSettings).columnDelimiter'
                            type: 'Expression'
                          }
                          cw_escapeChar: {
                            value: '@json(item().SinkObjectSettings).escapeChar'
                            type: 'Expression'
                          }
                          cw_quoteChar: {
                            value: '@json(item().SinkObjectSettings).quoteChar'
                            type: 'Expression'
                          }
                          cw_firstRowAsHeaderSource: {
                            value: '@json(item().SourceObjectSettings).firstRowAsHeaderSource'
                            type: 'Expression'
                          }
                        }
                      }
                    ]
                    outputs: [
                      {
                        referenceName: 'MetadataDrivenCopy_FileSystem_DestinationDS'
                        type: 'DatasetReference'
                        parameters: {
                          cw_compressionCodec: {
                            value: '@json(item().SinkObjectSettings).compressionCodec'
                            type: 'Expression'
                          }
                          cw_compressionLevel: {
                            value: '@json(item().SinkObjectSettings).compressionLevel'
                            type: 'Expression'
                          }
                          cw_columnDelimiter: {
                            value: '@json(item().SinkObjectSettings).columnDelimiter'
                            type: 'Expression'
                          }
                          cw_escapeChar: {
                            value: '@json(item().SinkObjectSettings).escapeChar'
                            type: 'Expression'
                          }
                          cw_quoteChar: {
                            value: '@json(item().SinkObjectSettings).quoteChar'
                            type: 'Expression'
                          }
                          cw_firstRowAsHeader: {
                            value: '@json(item().SinkObjectSettings).firstRowAsHeader'
                            type: 'Expression'
                          }
                          cw_fileName: {
                            value: '@json(item().SinkObjectSettings).fileName'
                            type: 'Expression'
                          }
                          cw_folderPath: {
                            value: '@json(item().SinkObjectSettings).folderPath'
                            type: 'Expression'
                          }
                          cw_container: {
                            value: '@json(item().SinkObjectSettings).container'
                            type: 'Expression'
                          }
                        }
                      }
                    ]
                  }
                  {
                    name: 'DefaultOneObject_Parquet'
                    description: 'Take a full snapshot on this object and copy it to the destination'
                    type: 'Copy'
                    dependsOn: [
                      {
                        activity: 'LogParquetCommence_FL_copy1'
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
                    userProperties: [
                      {
                        name: 'Source'
                        value: '@{json(item().ParquetSourceObjectSettings).fileSystem}/@{json(item().ParquetSourceObjectSettings).folderPath}/@{json(item().ParquetSourceObjectSettings).fileName}'
                      }
                      {
                        name: 'Destination'
                        value: '@{json(item().ParquetSinkObjectSettings).fileSystem}/@{json(item().ParquetSinkObjectSettings).folderPath}/@{json(item().ParquetSinkObjectSettings).fileName}'
                      }
                    ]
                    typeProperties: {
                      source: {
                        type: 'DelimitedTextSource'
                        additionalColumns: [
                          {
                            name: 'loadDate'
                            value: {
                              value: '@formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyyMMdd\')'
                              type: 'Expression'
                            }
                          }
                        ]
                        storeSettings: {
                          type: 'AzureBlobStorageReadSettings'
                          recursive: true
                          enablePartitionDiscovery: false
                        }
                        formatSettings: {
                          type: 'DelimitedTextReadSettings'
                        }
                      }
                      sink: {
                        type: 'ParquetSink'
                        storeSettings: {
                          type: 'AzureBlobStorageWriteSettings'
                          copyBehavior: {
                            value: '@json(item().ParquetCopySinkSettings).copyBehavior'
                            type: 'Expression'
                          }
                        }
                        formatSettings: {
                          type: 'ParquetWriteSettings'
                        }
                      }
                      enableStaging: false
                      validateDataConsistency: true
                      translator: {
                        value: '@json(item().ParquetCopyActivitySettings).translator'
                        type: 'Expression'
                      }
                    }
                    inputs: [
                      {
                        referenceName: 'MetadataDrivenCopy_FileSystem_ParquetSourceDS'
                        type: 'DatasetReference'
                        parameters: {
                          cw_columnDelimiter: {
                            value: '@json(item().ParquetSourceObjectSettings).columnDelimiter'
                            type: 'Expression'
                          }
                          cw_escapeChar: {
                            value: '@json(item().ParquetSourceObjectSettings).escapeChar'
                            type: 'Expression'
                          }
                          cw_quoteChar: {
                            value: '@json(item().ParquetSourceObjectSettings).quoteChar'
                            type: 'Expression'
                          }
                          cw_firstRowAsHeader: {
                            value: '@json(item().ParquetSourceObjectSettings).firstRowAsHeader'
                            type: 'Expression'
                          }
                          cw_fileName: {
                            value: '@json(item().ParquetSourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          cw_folderPath: {
                            value: '@json(item().ParquetSourceObjectSettings).folderPath'
                            type: 'Expression'
                          }
                          cw_fileSystem: {
                            value: '@json(item().ParquetSourceObjectSettings).fileSystem'
                            type: 'Expression'
                          }
                          cw_container: {
                            value: '@json(item().ParquetSourceObjectSettings).container'
                            type: 'Expression'
                          }
                        }
                      }
                    ]
                    outputs: [
                      {
                        referenceName: 'MetadataDrivenCopy_FileSystem_ParquetDestinationDS'
                        type: 'DatasetReference'
                        parameters: {
                          cw_compressionCodec: {
                            value: '@json(item().ParquetSinkObjectSettings).compressionCodec'
                            type: 'Expression'
                          }
                          cw_columnDelimiter: {
                            value: '@json(item().ParquetSinkObjectSettings).columnDelimiter'
                            type: 'Expression'
                          }
                          cw_escapeChar: {
                            value: '@json(item().ParquetSinkObjectSettings).escapeChar'
                            type: 'Expression'
                          }
                          cw_quoteChar: {
                            value: '@json(item().ParquetSinkObjectSettings).quoteChar'
                            type: 'Expression'
                          }
                          cw_fileName: {
                            value: '@json(item().ParquetSinkObjectSettings).fileName'
                            type: 'Expression'
                          }
                          cw_folderPath: {
                            value: '@json(item().ParquetSinkObjectSettings).folderPath'
                            type: 'Expression'
                          }
                          cw_fileSystem: {
                            value: '@json(item().ParquetSinkObjectSettings).fileSystem'
                            type: 'Expression'
                          }
                          cw_container: {
                            value: '@json(item().ParquetSinkObjectSettings).container'
                            type: 'Expression'
                          }
                        }
                      }
                    ]
                  }
                  {
                    name: 'LogCSVCommence'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'csv copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: null
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'commenced'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).sheetName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'LogCSVSuccess'
                    type: 'SqlServerStoredProcedure'
                    dependsOn: [
                      {
                        activity: 'DefaultOneObject'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'csv copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: null
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'succeeded'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).sheetName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                        rowsCopied: {
                          value: {
                            value: '@activity(\'DefaultOneObject\').output?.rowsCopied'
                            type: 'Expression'
                          }
                          type: 'Int64'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'LogParquetCommence_FL_copy1'
                    type: 'SqlServerStoredProcedure'
                    dependsOn: [
                      {
                        activity: 'LogCSVSuccess'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'parquet copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: null
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'commenced'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).sheetName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'LogCSVFail'
                    type: 'SqlServerStoredProcedure'
                    dependsOn: [
                      {
                        activity: 'DefaultOneObject'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'csv copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: {
                            value: '@activity(\'DefaultOneObject\').error?.message'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'failed'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).sheetName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'Fail_LogCSVFail'
                    type: 'Fail'
                    dependsOn: [
                      {
                        activity: 'LogCSVFail'
                        dependencyConditions: [
                          'Completed'
                        ]
                      }
                    ]
                    userProperties: []
                    typeProperties: {
                      message: {
                        value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                        type: 'Expression'
                      }
                      errorCode: '3204'
                    }
                  }
                  {
                    name: 'LogParquetSuccess'
                    type: 'SqlServerStoredProcedure'
                    dependsOn: [
                      {
                        activity: 'DefaultOneObject_Parquet'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'parquet copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: null
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'succeeded'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).sheetName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                        rowsCopied: {
                          value: {
                            value: '@activity(\'DefaultOneObject_Parquet\').output?.rowsCopied'
                            type: 'Expression'
                          }
                          type: 'Int64'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'LogParquetFail'
                    type: 'SqlServerStoredProcedure'
                    dependsOn: [
                      {
                        activity: 'DefaultOneObject_Parquet'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'parquet copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: {
                            value: '@activity(\'DefaultOneObject_Parquet\').error?.message'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'failed'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).sheetName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'Fail_LogParquetFail'
                    type: 'Fail'
                    dependsOn: [
                      {
                        activity: 'LogParquetFail'
                        dependencyConditions: [
                          'Completed'
                        ]
                      }
                    ]
                    userProperties: []
                    typeProperties: {
                      message: {
                        value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                        type: 'Expression'
                      }
                      errorCode: '3204'
                    }
                  }
                ]
              }
            }
            {
              name: 'GetSourceConnectionValues'
              type: 'Lookup'
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
                source: {
                  type: 'AzureSqlSource'
                  sqlReaderQuery: {
                    value: 'select ConnectionSettings from @{pipeline().parameters.ConnectionControlTableName} where Name = \'@{item().SourceConnectionSettingsName} \' AND Id = @{item().Id}'
                    type: 'Expression'
                  }
                  partitionOption: 'None'
                }
                dataset: {
                  referenceName: 'MetadataDrivenCopy_SQL_ControlDS'
                  type: 'DatasetReference'
                  parameters: {}
                }
                firstRowOnly: false
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
      ObjectsPerGroupToCopy: {
        type: 'Array'
      }
      ConnectionControlTableName: {
        type: 'String'
      }
      TriggerName: {
        type: 'string'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_FileSystem'
    }
    annotations: []
    lastPublishTime: '2022-05-23T04:36:04Z'
  }
  dependsOn: [
    '${factoryId}/datasets/MetadataDrivenCopy_SQL_ControlDS'
    '${factoryId}/datasets/MetadataDrivenCopy_FileSystem_SourceDS'
    '${factoryId}/datasets/MetadataDrivenCopy_FileSystem_DestinationDS'
    '${factoryId}/datasets/MetadataDrivenCopy_FileSystem_ParquetSourceDS'
    '${factoryId}/datasets/MetadataDrivenCopy_FileSystem_ParquetDestinationDS'
    '${factoryId}/linkedServices/ls_azsqldb_metadatacontroldb'
  ]
}

resource factoryName_MetadataDrivenCopy_FlatFile_MiddleLevel 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_FlatFile_MiddleLevel'
  properties: {
    description: 'This pipeline will copy one batch of objects. The objects belonging to this batch will be copied parallelly.'
    activities: [
      {
        name: 'DivideOneBatchIntoMultipleGroups'
        description: 'Divide objects from single batch into multiple sub parallel groups to avoid reaching the output limit of lookup activity.'
        type: 'ForEach'
        dependsOn: []
        userProperties: []
        typeProperties: {
          items: {
            value: '@range(0, add(div(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity),\n                    if(equals(mod(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity), 0), 0, 1)))'
            type: 'Expression'
          }
          isSequential: false
          batchCount: 50
          activities: [
            {
              name: 'GetObjectsPerGroupToCopy'
              description: 'Get objects (tables etc.) from control table required to be copied in this group. The order of objects to be copied following the TaskId in control table (ORDER BY [TaskId] DESC).'
              type: 'Lookup'
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
                source: {
                  type: 'AzureSqlSource'
                  sqlReaderQuery: {
                    value: 'WITH OrderedControlTable AS (\n                             SELECT *, ROW_NUMBER() OVER (ORDER BY [TaskId], [Id] DESC) AS RowNumber\n                             FROM @{pipeline().parameters.MainControlTableName}\n                             where TopLevelPipelineName = \'@{pipeline().parameters.TopLevelPipelineName}\'\n                             and TriggerName like \'%@{pipeline().parameters.TriggerName}%\' and CopyEnabled = 1)\n                             SELECT * FROM OrderedControlTable WHERE RowNumber BETWEEN @{add(mul(int(item()),pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity),\n                             add(mul(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.CurrentSequentialNumberOfBatch), 1))}\n                             AND @{min(add(mul(int(item()), pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity), add(mul(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.CurrentSequentialNumberOfBatch),\n                             pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity)),\n                            mul(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, add(pipeline().parameters.CurrentSequentialNumberOfBatch,1)), pipeline().parameters.SumOfObjectsToCopy)}'
                    type: 'Expression'
                  }
                  partitionOption: 'None'
                }
                dataset: {
                  referenceName: 'MetadataDrivenCopy_Oracle_ControlDS'
                  type: 'DatasetReference'
                  parameters: {}
                }
                firstRowOnly: false
              }
            }
            {
              name: 'CopyObjectsInOneGroup'
              description: 'Execute another pipeline to copy objects from one group. The objects belonging to this group will be copied parallelly.'
              type: 'ExecutePipeline'
              dependsOn: [
                {
                  activity: 'GetObjectsPerGroupToCopy'
                  dependencyConditions: [
                    'Succeeded'
                  ]
                }
              ]
              userProperties: []
              typeProperties: {
                pipeline: {
                  referenceName: 'MetadataDrivenCopy_FlatFile_BottomLevel'
                  type: 'PipelineReference'
                }
                waitOnCompletion: true
                parameters: {
                  ObjectsPerGroupToCopy: {
                    value: '@activity(\'GetObjectsPerGroupToCopy\').output.value'
                    type: 'Expression'
                  }
                  ConnectionControlTableName: {
                    value: '@pipeline().parameters.ConnectionControlTableName'
                    type: 'Expression'
                  }
                  TriggerName: {
                    value: '@pipeline().parameters.TriggerName'
                    type: 'Expression'
                  }
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
      MaxNumberOfObjectsReturnedFromLookupActivity: {
        type: 'Int'
      }
      TopLevelPipelineName: {
        type: 'String'
      }
      TriggerName: {
        type: 'String'
      }
      CurrentSequentialNumberOfBatch: {
        type: 'Int'
      }
      SumOfObjectsToCopy: {
        type: 'Int'
      }
      SumOfObjectsToCopyForCurrentBatch: {
        type: 'Int'
      }
      MainControlTableName: {
        type: 'String'
      }
      ConnectionControlTableName: {
        type: 'String'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_FileSystem'
    }
    annotations: []
    lastPublishTime: '2022-05-11T07:03:14Z'
  }
  dependsOn: [
    '${factoryId}/datasets/MetadataDrivenCopy_Oracle_ControlDS'
    '${factoryId}/pipelines/MetadataDrivenCopy_FlatFile_BottomLevel'
  ]
}

resource factoryName_MetadataDrivenCopy_FlatFile_TopLevel 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_FlatFile_TopLevel'
  properties: {
    description: 'This pipeline will count the total number of objects (tables etc.) required to be copied in this run, come up with the number of sequential batches based on the max allowed concurrent copy task, and then execute another pipeline to copy different batches sequentially.'
    activities: [
      {
        name: 'GetSumOfObjectsToCopy'
        description: 'Count the total number of objects (tables etc.) required to be copied in this run.'
        type: 'Lookup'
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
          source: {
            type: 'AzureSqlSource'
            sqlReaderQuery: {
              value: 'SELECT count(*) as count FROM @{pipeline().parameters.MainControlTableName} where TopLevelPipelineName=\'@{pipeline().Pipeline}\' and TriggerName like \'%@{pipeline().TriggerName}%\' and CopyEnabled = 1'
              type: 'Expression'
            }
            partitionOption: 'None'
          }
          dataset: {
            referenceName: 'MetadataDrivenCopy_Oracle_ControlDS'
            type: 'DatasetReference'
            parameters: {}
          }
        }
      }
      {
        name: 'CopyBatchesOfObjectsSequentially'
        description: 'Come up with the number of sequential batches based on the max allowed concurrent copy tasks, and then execute another pipeline to copy different batches sequentially.'
        type: 'ForEach'
        dependsOn: [
          {
            activity: 'GetSumOfObjectsToCopy'
            dependencyConditions: [
              'Succeeded'
            ]
          }
        ]
        userProperties: []
        typeProperties: {
          items: {
            value: '@range(0, add(div(activity(\'GetSumOfObjectsToCopy\').output.firstRow.count,\n                    pipeline().parameters.MaxNumberOfConcurrentTasks),\n                    if(equals(mod(activity(\'GetSumOfObjectsToCopy\').output.firstRow.count,\n                    pipeline().parameters.MaxNumberOfConcurrentTasks), 0), 0, 1)))'
            type: 'Expression'
          }
          isSequential: true
          activities: [
            {
              name: 'CopyObjectsInOneBatch'
              description: 'Execute another pipeline to copy one batch of objects. The objects belonging to this batch will be copied parallelly.'
              type: 'ExecutePipeline'
              dependsOn: []
              userProperties: []
              typeProperties: {
                pipeline: {
                  referenceName: 'MetadataDrivenCopy_FlatFile_MiddleLevel'
                  type: 'PipelineReference'
                }
                waitOnCompletion: true
                parameters: {
                  MaxNumberOfObjectsReturnedFromLookupActivity: {
                    value: '@pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity'
                    type: 'Expression'
                  }
                  TopLevelPipelineName: {
                    value: '@{pipeline().Pipeline}'
                    type: 'Expression'
                  }
                  TriggerName: {
                    value: '@{pipeline().TriggerName}'
                    type: 'Expression'
                  }
                  CurrentSequentialNumberOfBatch: {
                    value: '@item()'
                    type: 'Expression'
                  }
                  SumOfObjectsToCopy: {
                    value: '@activity(\'GetSumOfObjectsToCopy\').output.firstRow.count'
                    type: 'Expression'
                  }
                  SumOfObjectsToCopyForCurrentBatch: {
                    value: '@min(pipeline().parameters.MaxNumberOfConcurrentTasks, activity(\'GetSumOfObjectsToCopy\').output.firstRow.count)'
                    type: 'Expression'
                  }
                  MainControlTableName: {
                    value: '@pipeline().parameters.MainControlTableName'
                    type: 'Expression'
                  }
                  ConnectionControlTableName: {
                    value: '@pipeline().parameters.ConnectionControlTableName'
                    type: 'Expression'
                  }
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
      MaxNumberOfObjectsReturnedFromLookupActivity: {
        type: 'Int'
        defaultValue: 5000
      }
      MaxNumberOfConcurrentTasks: {
        type: 'Int'
        defaultValue: 16
      }
      MainControlTableName: {
        type: 'String'
        defaultValue: 'config.FileSystemControlTable'
      }
      ConnectionControlTableName: {
        type: 'String'
        defaultValue: 'config.FileSystemConnectionControlTable'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_FileSystem'
    }
    annotations: [
      'MetadataDrivenSolution'
    ]
    lastPublishTime: '2022-05-12T23:34:19Z'
  }
  dependsOn: [
    '${factoryId}/datasets/MetadataDrivenCopy_Oracle_ControlDS'
    '${factoryId}/pipelines/MetadataDrivenCopy_FlatFile_MiddleLevel'
  ]
}

resource factoryName_MetadataDrivenCopy_SQL_BottomLevel 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_SQL_BottomLevel'
  properties: {
    description: 'This pipeline will copy objects from one group. The objects belonging to this group will be copied parallelly.'
    activities: [
      {
        name: 'ListObjectsFromOneGroup'
        description: 'List objects from one group and iterate each of them to downstream activities'
        type: 'ForEach'
        dependsOn: []
        userProperties: []
        typeProperties: {
          items: {
            value: '@pipeline().parameters.ObjectsPerGroupToCopy'
            type: 'Expression'
          }
          activities: [
            {
              name: 'RouteJobsBasedOnLoadingBehavior'
              description: 'Check the loading behavior for each object if it requires full load or incremental load. If it is Default or FullLoad case, do full load. If it is DeltaLoad case, do incremental load.'
              type: 'Switch'
              dependsOn: [
                {
                  activity: 'GetTableMetadataInfo'
                  dependencyConditions: [
                    'Succeeded'
                  ]
                }
              ]
              userProperties: []
              typeProperties: {
                on: {
                  value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                  type: 'Expression'
                }
                cases: [
                  {
                    value: 'FullLoad'
                    activities: [
                      {
                        name: 'FullLoadOneObject'
                        description: 'Take a full snapshot on this object and copy it to the destination'
                        type: 'Copy'
                        dependsOn: [
                          {
                            activity: 'LogCSVCommence_FL'
                            dependencyConditions: [
                              'Succeeded'
                            ]
                          }
                        ]
                        policy: {
                          timeout: '7.00:00:00'
                          retry: 1
                          retryIntervalInSeconds: 300
                          secureOutput: false
                          secureInput: false
                        }
                        userProperties: [
                          {
                            name: 'Source'
                            value: '@{json(item().SourceObjectSettings).schema}.@{json(item().SourceObjectSettings).table}'
                          }
                          {
                            name: 'Destination'
                            value: '@{json(item().SinkObjectSettings).container}/@{json(item().SinkObjectSettings).folderPath}/@{json(item().SinkObjectSettings).fileName}'
                          }
                        ]
                        typeProperties: {
                          source: {
                            type: 'SqlServerSource'
                            sqlReaderQuery: {
                              value: '@json(item().CopySourceSettings).sqlReaderQuery'
                              type: 'Expression'
                            }
                            partitionOption: {
                              value: '@json(item().CopySourceSettings).partitionOption'
                              type: 'Expression'
                            }
                            partitionSettings: {
                              partitionColumnName: {
                                value: '@json(item().CopySourceSettings).partitionColumnName'
                                type: 'Expression'
                              }
                              partitionUpperBound: {
                                value: '@json(item().CopySourceSettings).partitionUpperBound'
                                type: 'Expression'
                              }
                              partitionLowerBound: {
                                value: '@json(item().CopySourceSettings).partitionLowerBound'
                                type: 'Expression'
                              }
                              partitionNames: '@json(item().CopySourceSettings).partitionNames'
                            }
                          }
                          sink: {
                            type: 'DelimitedTextSink'
                            storeSettings: {
                              type: 'AzureBlobFSWriteSettings'
                            }
                            formatSettings: {
                              type: 'DelimitedTextWriteSettings'
                              quoteAllText: true
                              fileExtension: '.txt'
                            }
                          }
                          enableStaging: false
                          validateDataConsistency: true
                          translator: {
                            value: '@json(item().CopyActivitySettings).translator'
                            type: 'Expression'
                          }
                        }
                        inputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_SQL_SourceDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_schema: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              cw_table: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              cw_ls_serverName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).serverName'
                                type: 'Expression'
                              }
                              cw_ls_databaseName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).databaseName'
                                type: 'Expression'
                              }
                              cw_ls_userName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).userName'
                                type: 'Expression'
                              }
                              cw_ls_passwordSecretName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).passwordSecretName'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                        outputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_SQL_DestinationDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_compressionCodec: {
                                value: '@json(item().SinkObjectSettings).compressionCodec'
                                type: 'Expression'
                              }
                              cw_compressionLevel: {
                                value: '@json(item().SinkObjectSettings).compressionLevel'
                                type: 'Expression'
                              }
                              cw_columnDelimiter: {
                                value: '@json(item().SinkObjectSettings).columnDelimiter'
                                type: 'Expression'
                              }
                              cw_escapeChar: {
                                value: '@json(item().SinkObjectSettings).escapeChar'
                                type: 'Expression'
                              }
                              cw_quoteChar: {
                                value: '@json(item().SinkObjectSettings).quoteChar'
                                type: 'Expression'
                              }
                              cw_firstRowAsHeader: {
                                value: '@json(item().SinkObjectSettings).firstRowAsHeader'
                                type: 'Expression'
                              }
                              cw_fileName: {
                                value: '@concat(json(item().SinkObjectSettings).fileName, \'.csv\')'
                                type: 'Expression'
                              }
                              cw_folderPath: {
                                value: '@concat(json(item().SinkObjectSettings).folderPath, \'/\', json(item().DataLoadingBehaviorSettings).dataLoadingBehavior)'
                                type: 'Expression'
                              }
                              cw_container: {
                                value: '@json(item().SinkObjectSettings).container'
                                type: 'Expression'
                              }
                              cw_pipelineStartTime: {
                                value: '@pipeline().parameters.PipelineStartTime'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                      }
                      {
                        name: 'FullLoadOneObject_Parquet'
                        description: 'Take a full snapshot on this object and copy it to the destination'
                        type: 'Copy'
                        dependsOn: [
                          {
                            activity: 'LogParquetCommence_FL'
                            dependencyConditions: [
                              'Succeeded'
                            ]
                          }
                        ]
                        policy: {
                          timeout: '7.00:00:00'
                          retry: 1
                          retryIntervalInSeconds: 300
                          secureOutput: false
                          secureInput: false
                        }
                        userProperties: [
                          {
                            name: 'Source'
                            value: '@{json(item().ParquetSourceObjectSettings).fileSystem}/@{json(item().ParquetSourceObjectSettings).folderPath}/@{json(item().ParquetSourceObjectSettings).fileName}'
                          }
                          {
                            name: 'Destination'
                            value: '@{json(item().ParquetSinkObjectSettings).fileSystem}/@{json(item().ParquetSinkObjectSettings).folderPath}/@{json(item().ParquetSinkObjectSettings).fileName}'
                          }
                        ]
                        typeProperties: {
                          source: {
                            type: 'DelimitedTextSource'
                            additionalColumns: [
                              {
                                name: 'loadDate'
                                value: {
                                  value: '@formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyyMMdd\')'
                                  type: 'Expression'
                                }
                              }
                            ]
                            storeSettings: {
                              type: 'AzureBlobFSReadSettings'
                              recursive: {
                                value: '@json(item().ParquetCopySourceSettings).recursive'
                                type: 'Expression'
                              }
                            }
                            formatSettings: {
                              type: 'DelimitedTextReadSettings'
                              skipLineCount: {
                                value: '@json(item().ParquetCopySourceSettings).skipLineCount'
                                type: 'Expression'
                              }
                            }
                          }
                          sink: {
                            type: 'ParquetSink'
                            storeSettings: {
                              type: 'AzureBlobFSWriteSettings'
                            }
                            formatSettings: {
                              type: 'ParquetWriteSettings'
                            }
                          }
                          enableStaging: false
                          validateDataConsistency: true
                          translator: {
                            value: '@json(item().ParquetCopyActivitySettings).translator'
                            type: 'Expression'
                          }
                        }
                        inputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_SQL_ParquetSourceDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_columnDelimiter: {
                                value: '@json(item().ParquetSourceObjectSettings).columnDelimiter'
                                type: 'Expression'
                              }
                              cw_escapeChar: {
                                value: '@json(item().ParquetSourceObjectSettings).escapeChar'
                                type: 'Expression'
                              }
                              cw_quoteChar: {
                                value: '@json(item().ParquetSourceObjectSettings).quoteChar'
                                type: 'Expression'
                              }
                              cw_firstRowAsHeader: {
                                value: '@json(item().ParquetSourceObjectSettings).firstRowAsHeader'
                                type: 'Expression'
                              }
                              cw_fileName: {
                                value: '@concat(json(item().ParquetSourceObjectSettings).fileName, \'.csv\')'
                                type: 'Expression'
                              }
                              cw_folderPath: {
                                value: '@concat(json(item().ParquetSourceObjectSettings).folderPath, \'/\', json(item().DataLoadingBehaviorSettings).dataLoadingBehavior)'
                                type: 'Expression'
                              }
                              cw_fileSystem: {
                                value: '@json(item().ParquetSourceObjectSettings).fileSystem'
                                type: 'Expression'
                              }
                              cw_pipelineStartTime: {
                                value: '@pipeline().parameters.PipelineStartTime'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                        outputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_SQL_ParquetDestinationDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_compressionCodec: {
                                value: '@json(item().ParquetSinkObjectSettings).compressionCodec'
                                type: 'Expression'
                              }
                              cw_columnDelimiter: {
                                value: '@json(item().ParquetSinkObjectSettings).columnDelimiter'
                                type: 'Expression'
                              }
                              cw_escapeChar: {
                                value: '@json(item().ParquetSinkObjectSettings).escapeChar'
                                type: 'Expression'
                              }
                              cw_quoteChar: {
                                value: '@json(item().ParquetSinkObjectSettings).quoteChar'
                                type: 'Expression'
                              }
                              cw_fileName: {
                                value: '@concat(json(item().ParquetSinkObjectSettings).fileName, \'.parquet\')'
                                type: 'Expression'
                              }
                              cw_folderPath: {
                                value: '@concat(json(item().ParquetSinkObjectSettings).folderPath, \'/\', json(item().DataLoadingBehaviorSettings).dataLoadingBehavior)'
                                type: 'Expression'
                              }
                              cw_fileSystem: {
                                value: '@json(item().ParquetSinkObjectSettings).fileSystem'
                                type: 'Expression'
                              }
                              cw_pipelineStartTime: {
                                value: '@pipeline().parameters.PipelineStartTime'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                      }
                      {
                        name: 'LogCSVCommence_FL'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'csv copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'commenced'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogCSVSuccess_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'FullLoadOneObject'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'csv copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'succeeded'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: {
                                value: '@activity(\'FullLoadOneObject\').output?.rowsCopied'
                                type: 'Expression'
                              }
                              type: 'Int64'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogParquetCommence_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'LogCSVSuccess_FL'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'parquet copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'commenced'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogCSVFail_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'FullLoadOneObject'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'csv copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: {
                                value: '@activity(\'FullLoadOneObject\').error?.message'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'failed'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogParquetSuccess_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'FullLoadOneObject_Parquet'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'parquet copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'succeeded'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: {
                                value: '@activity(\'FullLoadOneObject_Parquet\').output?.rowsCopied'
                                type: 'Expression'
                              }
                              type: 'Int64'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogParquetFail_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'FullLoadOneObject_Parquet'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'parquet copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: {
                                value: '@activity(\'FullLoadOneObject_Parquet\').error?.message'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'failed'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'Fail_LogCSVFail_FL'
                        type: 'Fail'
                        dependsOn: [
                          {
                            activity: 'LogCSVFail_FL'
                            dependencyConditions: [
                              'Completed'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          message: {
                            value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                            type: 'Expression'
                          }
                          errorCode: '3204'
                        }
                      }
                      {
                        name: 'Fail_LogParquetFail_FL'
                        type: 'Fail'
                        dependsOn: [
                          {
                            activity: 'LogParquetFail_FL'
                            dependencyConditions: [
                              'Completed'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          message: {
                            value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                            type: 'Expression'
                          }
                          errorCode: '3204'
                        }
                      }
                      {
                        name: 'ExternalTableAndViewsCreation'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'LogParquetSuccess_FL'
                            dependencyConditions: [
                              'Succeeded'
                            ]
                          }
                        ]
                        policy: {
                          timeout: '0.12:00:00'
                          retry: 0
                          retryIntervalInSeconds: 30
                          secureOutput: false
                          secureInput: false
                        }
                        userProperties: []
                        typeProperties: {
                          storedProcedureName: '[helper].[usp_GenerateDDL_Raw]'
                          storedProcedureParameters: {
                            DATATYPES: {
                              value: {
                                value: '@activity(\'GetTableMetadataInfo\').output.value[0].json_string'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            DATETIMEPATH: {
                              value: {
                                value: '@if(empty(pipeline().parameters.PipelineStartTime),concat(formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')),concat(formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')))'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            FILENAME: {
                              value: {
                                value: '@json(item().ParquetSinkObjectSettings).fileName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            SCHEMA: {
                              value: {
                                value: '@split(json(item().ParquetSinkObjectSettings).folderPath, \'/\')[0]'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            STORAGEACCOUNT: {
                              value: {
                                value: '@json(item().SourceObjectSettings).StorageAccount'
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
                  }
                  {
                    value: 'DeltaLoad'
                    activities: [
                      {
                        name: 'GetMaxWatermarkValue'
                        description: 'Query the source object to get the max value from watermark column'
                        type: 'Lookup'
                        dependsOn: [
                          {
                            activity: 'LogCSVCommence_DL'
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
                          source: {
                            type: 'SqlServerSource'
                            sqlReaderQuery: {
                              value: 'select max([@{json(item().DataLoadingBehaviorSettings).watermarkColumnName}]) as CurrentMaxWaterMarkColumnValue from [@{json(item().SourceObjectSettings).schema}].[@{json(item().SourceObjectSettings).table}]'
                              type: 'Expression'
                            }
                            partitionOption: 'None'
                          }
                          dataset: {
                            referenceName: 'MetadataDrivenCopy_SQL_SourceDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_schema: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              cw_table: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              cw_ls_serverName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).serverName'
                                type: 'Expression'
                              }
                              cw_ls_databaseName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).databaseName'
                                type: 'Expression'
                              }
                              cw_ls_userName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).userName'
                                type: 'Expression'
                              }
                              cw_ls_passwordSecretName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).passwordSecretName'
                                type: 'Expression'
                              }
                            }
                          }
                        }
                      }
                      {
                        name: 'DeltaLoadOneObject'
                        description: 'Copy the changed data only from last time via comparing the value in watermark column to identify changes.'
                        type: 'Copy'
                        dependsOn: [
                          {
                            activity: 'GetMaxWatermarkValue'
                            dependencyConditions: [
                              'Succeeded'
                            ]
                          }
                        ]
                        policy: {
                          timeout: '7.00:00:00'
                          retry: 1
                          retryIntervalInSeconds: 300
                          secureOutput: false
                          secureInput: false
                        }
                        userProperties: [
                          {
                            name: 'Source'
                            value: '@{json(item().SourceObjectSettings).schema}.@{json(item().SourceObjectSettings).table}'
                          }
                          {
                            name: 'Destination'
                            value: '@{json(item().SinkObjectSettings).container}/@{json(item().SinkObjectSettings).folderPath}/@{json(item().SinkObjectSettings).fileName}'
                          }
                        ]
                        typeProperties: {
                          source: {
                            type: 'SqlServerSource'
                            sqlReaderQuery: {
                              value: 'select * from [@{json(item().SourceObjectSettings).schema}].[@{json(item().SourceObjectSettings).table}] \n    where [@{json(item().DataLoadingBehaviorSettings).watermarkColumnName}] > @{if(contains(json(item().DataLoadingBehaviorSettings), \'watermarkColumnStartValue\'), if(contains(json(item().DataLoadingBehaviorSettings).watermarkColumnType, \'Int\'),\n    json(item().DataLoadingBehaviorSettings).watermarkColumnStartValue, \n    concat(\'\'\'\', json(item().DataLoadingBehaviorSettings).watermarkColumnStartValue, \'\'\'\')), concat(\'\'\'1900-01-01\'\'\'))}\n    and [@{json(item().DataLoadingBehaviorSettings).watermarkColumnName}] <= @{if(contains(json(item().DataLoadingBehaviorSettings).watermarkColumnType, \'Int\'),\n    activity(\'GetMaxWatermarkValue\').output.firstRow.CurrentMaxWaterMarkColumnValue, \n    concat(\'\'\'\', activity(\'GetMaxWatermarkValue\').output.firstRow.CurrentMaxWaterMarkColumnValue, \'\'\'\'))}'
                              type: 'Expression'
                            }
                            partitionOption: {
                              value: '@json(item().CopySourceSettings).partitionOption'
                              type: 'Expression'
                            }
                            partitionSettings: {
                              partitionColumnName: {
                                value: '@{json(item().CopySourceSettings).partitionColumnName}'
                                type: 'Expression'
                              }
                              partitionUpperBound: {
                                value: '@{json(item().CopySourceSettings).partitionUpperBound}'
                                type: 'Expression'
                              }
                              partitionLowerBound: {
                                value: '@{json(item().CopySourceSettings).partitionLowerBound}'
                                type: 'Expression'
                              }
                              partitionNames: '@json(item().CopySourceSettings).partitionNames'
                            }
                          }
                          sink: {
                            type: 'DelimitedTextSink'
                            storeSettings: {
                              type: 'AzureBlobFSWriteSettings'
                            }
                            formatSettings: {
                              type: 'DelimitedTextWriteSettings'
                              quoteAllText: true
                              fileExtension: '.txt'
                            }
                          }
                          enableStaging: false
                          validateDataConsistency: true
                          translator: {
                            value: '@json(item().CopyActivitySettings).translator'
                            type: 'Expression'
                          }
                        }
                        inputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_SQL_SourceDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_schema: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              cw_table: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              cw_ls_serverName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).serverName'
                                type: 'Expression'
                              }
                              cw_ls_databaseName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).databaseName'
                                type: 'Expression'
                              }
                              cw_ls_userName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).userName'
                                type: 'Expression'
                              }
                              cw_ls_passwordSecretName: {
                                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).passwordSecretName'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                        outputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_SQL_DestinationDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_compressionCodec: {
                                value: '@json(item().SinkObjectSettings).compressionCodec'
                                type: 'Expression'
                              }
                              cw_compressionLevel: {
                                value: '@json(item().SinkObjectSettings).compressionLevel'
                                type: 'Expression'
                              }
                              cw_columnDelimiter: {
                                value: '@json(item().SinkObjectSettings).columnDelimiter'
                                type: 'Expression'
                              }
                              cw_escapeChar: {
                                value: '@json(item().SinkObjectSettings).escapeChar'
                                type: 'Expression'
                              }
                              cw_quoteChar: {
                                value: '@json(item().SinkObjectSettings).quoteChar'
                                type: 'Expression'
                              }
                              cw_firstRowAsHeader: {
                                value: '@json(item().SinkObjectSettings).firstRowAsHeader'
                                type: 'Expression'
                              }
                              cw_fileName: {
                                value: '@concat(json(item().SinkObjectSettings).fileName, \'.csv\')'
                                type: 'Expression'
                              }
                              cw_folderPath: {
                                value: '@concat(json(item().SinkObjectSettings).folderPath, \'/\', json(item().DataLoadingBehaviorSettings).dataLoadingBehavior)'
                                type: 'Expression'
                              }
                              cw_container: {
                                value: '@json(item().SinkObjectSettings).container'
                                type: 'Expression'
                              }
                              cw_pipelineStartTime: {
                                value: '@pipeline().parameters.PipelineStartTime'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                      }
                      {
                        name: 'UpdateWatermarkColumnValue'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'DeltaLoadOneObject_Parquet'
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
                          storedProcedureName: '[config].[UpdateWatermarkColumnValue_SQL]'
                          storedProcedureParameters: {
                            Id: {
                              value: {
                                value: '@item().Id'
                                type: 'Expression'
                              }
                              type: 'Int32'
                            }
                            watermarkColumnStartValue: {
                              value: {
                                value: '@activity(\'GetMaxWatermarkValue\').output.firstRow.CurrentMaxWaterMarkColumnValue'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'DeltaLoadOneObject_Parquet'
                        description: 'Take a full snapshot on this object and copy it to the destination'
                        type: 'Copy'
                        dependsOn: [
                          {
                            activity: 'LogParquetCommence_DL'
                            dependencyConditions: [
                              'Succeeded'
                            ]
                          }
                        ]
                        policy: {
                          timeout: '7.00:00:00'
                          retry: 1
                          retryIntervalInSeconds: 300
                          secureOutput: false
                          secureInput: false
                        }
                        userProperties: [
                          {
                            name: 'Source'
                            value: '@{json(item().ParquetSourceObjectSettings).fileSystem}/@{json(item().ParquetSourceObjectSettings).folderPath}/@{json(item().ParquetSourceObjectSettings).fileName}'
                          }
                          {
                            name: 'Destination'
                            value: '@{json(item().ParquetSinkObjectSettings).fileSystem}/@{json(item().ParquetSinkObjectSettings).folderPath}/@{json(item().ParquetSinkObjectSettings).fileName}'
                          }
                        ]
                        typeProperties: {
                          source: {
                            type: 'DelimitedTextSource'
                            additionalColumns: [
                              {
                                name: 'loadDate'
                                value: {
                                  value: '@formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyyMMdd\')'
                                  type: 'Expression'
                                }
                              }
                            ]
                            storeSettings: {
                              type: 'AzureBlobFSReadSettings'
                              recursive: {
                                value: '@json(item().ParquetCopySourceSettings).recursive'
                                type: 'Expression'
                              }
                            }
                            formatSettings: {
                              type: 'DelimitedTextReadSettings'
                              skipLineCount: {
                                value: '@json(item().ParquetCopySourceSettings).skipLineCount'
                                type: 'Expression'
                              }
                            }
                          }
                          sink: {
                            type: 'ParquetSink'
                            storeSettings: {
                              type: 'AzureBlobFSWriteSettings'
                            }
                            formatSettings: {
                              type: 'ParquetWriteSettings'
                            }
                          }
                          enableStaging: false
                          validateDataConsistency: true
                          translator: {
                            value: '@json(item().ParquetCopyActivitySettings).translator'
                            type: 'Expression'
                          }
                        }
                        inputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_SQL_ParquetSourceDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_columnDelimiter: {
                                value: '@json(item().ParquetSourceObjectSettings).columnDelimiter'
                                type: 'Expression'
                              }
                              cw_escapeChar: {
                                value: '@json(item().ParquetSourceObjectSettings).escapeChar'
                                type: 'Expression'
                              }
                              cw_quoteChar: {
                                value: '@json(item().ParquetSourceObjectSettings).quoteChar'
                                type: 'Expression'
                              }
                              cw_firstRowAsHeader: {
                                value: '@json(item().ParquetSourceObjectSettings).firstRowAsHeader'
                                type: 'Expression'
                              }
                              cw_fileName: {
                                value: '@concat(json(item().ParquetSourceObjectSettings).fileName, \'.csv\')'
                                type: 'Expression'
                              }
                              cw_folderPath: {
                                value: '@concat(json(item().ParquetSourceObjectSettings).folderPath, \'/\', json(item().DataLoadingBehaviorSettings).dataLoadingBehavior)'
                                type: 'Expression'
                              }
                              cw_fileSystem: {
                                value: '@json(item().ParquetSourceObjectSettings).fileSystem'
                                type: 'Expression'
                              }
                              cw_pipelineStartTime: {
                                value: '@pipeline().parameters.PipelineStartTime'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                        outputs: [
                          {
                            referenceName: 'MetadataDrivenCopy_SQL_ParquetDestinationDS'
                            type: 'DatasetReference'
                            parameters: {
                              cw_compressionCodec: {
                                value: '@json(item().ParquetSinkObjectSettings).compressionCodec'
                                type: 'Expression'
                              }
                              cw_columnDelimiter: {
                                value: '@json(item().ParquetSinkObjectSettings).columnDelimiter'
                                type: 'Expression'
                              }
                              cw_escapeChar: {
                                value: '@json(item().ParquetSinkObjectSettings).escapeChar'
                                type: 'Expression'
                              }
                              cw_quoteChar: {
                                value: '@json(item().ParquetSinkObjectSettings).quoteChar'
                                type: 'Expression'
                              }
                              cw_fileName: {
                                value: '@concat(json(item().ParquetSinkObjectSettings).fileName, \'.parquet\')'
                                type: 'Expression'
                              }
                              cw_folderPath: {
                                value: '@concat(json(item().ParquetSinkObjectSettings).folderPath, \'/\', json(item().DataLoadingBehaviorSettings).dataLoadingBehavior)'
                                type: 'Expression'
                              }
                              cw_fileSystem: {
                                value: '@json(item().ParquetSinkObjectSettings).fileSystem'
                                type: 'Expression'
                              }
                              cw_pipelineStartTime: {
                                value: '@pipeline().parameters.PipelineStartTime'
                                type: 'Expression'
                              }
                            }
                          }
                        ]
                      }
                      {
                        name: 'LogCSVCommence_DL'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'csv copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'commenced'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogCSVSuccess_DL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'DeltaLoadOneObject'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'csv copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'succeeded'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: {
                                value: '@concat(json(item().DataLoadingBehaviorSettings).watermarkColumnName, \': \', if(contains(json(item().DataLoadingBehaviorSettings), \'watermarkColumnStartValue\'), json(item().DataLoadingBehaviorSettings).watermarkColumnStartValue, concat(\'1900-01-01\')))'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            rowsCopied: {
                              value: {
                                value: '@activity(\'DeltaLoadOneObject\').output?.rowsCopied'
                                type: 'Expression'
                              }
                              type: 'Int64'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogParquetCommence_DL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'LogCSVSuccess_DL'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'parquet copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'commenced'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogCSVFail_DL_2'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'DeltaLoadOneObject'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'csv copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: {
                                value: '@activity(\'DeltaLoadOneObject\').error?.message'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'failed'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: {
                                value: '@concat(json(item().DataLoadingBehaviorSettings).watermarkColumnName, \': \', json(item().DataLoadingBehaviorSettings).watermarkColumnStartValue)'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogParquetSuccess_DL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'UpdateWatermarkColumnValue'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'parquet copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'succeeded'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: {
                                value: '@concat(json(item().DataLoadingBehaviorSettings).watermarkColumnName, \': \', if(contains(json(item().DataLoadingBehaviorSettings), \'watermarkColumnStartValue\'), json(item().DataLoadingBehaviorSettings).watermarkColumnStartValue, concat(\'1900-01-01\')))'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            rowsCopied: {
                              value: {
                                value: '@activity(\'DeltaLoadOneObject_Parquet\').output?.rowsCopied'
                                type: 'Expression'
                              }
                              type: 'Int64'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogParquetFail_DL_2'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'UpdateWatermarkColumnValue'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'parquet copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: {
                                value: '@activity(\'UpdateWatermarkColumnValue\').error?.message'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'failed'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: {
                                value: '@concat(json(item().DataLoadingBehaviorSettings).watermarkColumnName, \': \', json(item().DataLoadingBehaviorSettings).watermarkColumnStartValue)'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogParquetFail_DL_1'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'DeltaLoadOneObject_Parquet'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'parquet copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: {
                                value: '@activity(\'DeltaLoadOneObject_Parquet\').error?.message'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'failed'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: {
                                value: '@concat(json(item().DataLoadingBehaviorSettings).watermarkColumnName, \': \', json(item().DataLoadingBehaviorSettings).watermarkColumnStartValue)'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogCSVFail_DL_1'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'GetMaxWatermarkValue'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'csv copy'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: {
                                value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            errorText: {
                              value: {
                                value: '@activity(\'GetMaxWatermarkValue\').error?.message'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: {
                                value: '@item().SourceConnectionSettingsName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            status: {
                              value: 'failed'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: {
                                value: '@concat(json(item().DataLoadingBehaviorSettings).watermarkColumnName, \': \', json(item().DataLoadingBehaviorSettings).watermarkColumnStartValue)'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'Fail_LogCSVFail_DL_1'
                        type: 'Fail'
                        dependsOn: [
                          {
                            activity: 'LogCSVFail_DL_1'
                            dependencyConditions: [
                              'Completed'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          message: {
                            value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                            type: 'Expression'
                          }
                          errorCode: '3204'
                        }
                      }
                      {
                        name: 'Fail_LogParquetFail_DL_2'
                        type: 'Fail'
                        dependsOn: [
                          {
                            activity: 'LogParquetFail_DL_2'
                            dependencyConditions: [
                              'Completed'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          message: {
                            value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                            type: 'Expression'
                          }
                          errorCode: '3204'
                        }
                      }
                      {
                        name: 'Fail_LogParquetFail_DL_1'
                        type: 'Fail'
                        dependsOn: [
                          {
                            activity: 'LogParquetFail_DL_1'
                            dependencyConditions: [
                              'Completed'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          message: {
                            value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                            type: 'Expression'
                          }
                          errorCode: '3204'
                        }
                      }
                      {
                        name: 'Fail_LogCSVFail_DL_2'
                        type: 'Fail'
                        dependsOn: [
                          {
                            activity: 'LogCSVFail_DL_2'
                            dependencyConditions: [
                              'Completed'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          message: {
                            value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                            type: 'Expression'
                          }
                          errorCode: '3204'
                        }
                      }
                      {
                        name: 'SynapseViewsCreation'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'LogParquetSuccess_DL'
                            dependencyConditions: [
                              'Succeeded'
                            ]
                          }
                        ]
                        policy: {
                          timeout: '0.12:00:00'
                          retry: 0
                          retryIntervalInSeconds: 30
                          secureOutput: false
                          secureInput: false
                        }
                        userProperties: []
                        typeProperties: {
                          storedProcedureName: '[helper].[usp_GenerateDeltaLoadDDL_Raw]'
                          storedProcedureParameters: {
                            DATATYPES: {
                              value: {
                                value: '@activity(\'GetTableMetadataInfo\').output.value[0].json_string'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            DATETIMEPATH: {
                              value: {
                                value: '@if(empty(pipeline().parameters.PipelineStartTime),concat(formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')),concat(formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'MM\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'dd\'),\'/\',formatDateTime(convertTimeZone(pipeline().parameters.PipelineStartTime,\'UTC\',\'Cen. Australia Standard Time\'), \'HHmm\')))'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            FILENAME: {
                              value: {
                                value: '@json(item().ParquetSinkObjectSettings).fileName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            ROWKEY: {
                              value: {
                                value: '@{json(item().DataLoadingBehaviorSettings).RowKey}'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            SCHEMA: {
                              value: {
                                value: '@split(json(item().ParquetSinkObjectSettings).folderPath, \'/\')[0]'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            STORAGEACCOUNT: {
                              value: {
                                value: '@json(item().SourceObjectSettings).StorageAccount'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            WATERMARKCOLUMN: {
                              value: {
                                value: '@{json(item().DataLoadingBehaviorSettings).watermarkColumnName}'
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
                  }
                ]
                defaultActivities: [
                  {
                    name: 'DefaultFullLoadOneObject'
                    description: 'Take a full snapshot on this object and copy it to the destination'
                    type: 'Copy'
                    dependsOn: [
                      {
                        activity: 'LogCSVCommence'
                        dependencyConditions: [
                          'Succeeded'
                        ]
                      }
                    ]
                    policy: {
                      timeout: '7.00:00:00'
                      retry: 1
                      retryIntervalInSeconds: 300
                      secureOutput: false
                      secureInput: false
                    }
                    userProperties: [
                      {
                        name: 'Source'
                        value: '@{json(item().SourceObjectSettings).schema}.@{json(item().SourceObjectSettings).table}'
                      }
                      {
                        name: 'Destination'
                        value: '@{json(item().SinkObjectSettings).container}/@{json(item().SinkObjectSettings).folderPath}/@{json(item().SinkObjectSettings).fileName}'
                      }
                    ]
                    typeProperties: {
                      source: {
                        type: 'SqlServerSource'
                        sqlReaderQuery: {
                          value: '@json(item().CopySourceSettings).sqlReaderQuery'
                          type: 'Expression'
                        }
                        partitionOption: {
                          value: '@json(item().CopySourceSettings).partitionOption'
                          type: 'Expression'
                        }
                        partitionSettings: {
                          partitionColumnName: {
                            value: '@json(item().CopySourceSettings).partitionColumnName'
                            type: 'Expression'
                          }
                          partitionUpperBound: {
                            value: '@json(item().CopySourceSettings).partitionUpperBound'
                            type: 'Expression'
                          }
                          partitionLowerBound: {
                            value: '@json(item().CopySourceSettings).partitionLowerBound'
                            type: 'Expression'
                          }
                          partitionNames: '@json(item().CopySourceSettings).partitionNames'
                        }
                      }
                      sink: {
                        type: 'DelimitedTextSink'
                        storeSettings: {
                          type: 'AzureBlobFSWriteSettings'
                        }
                        formatSettings: {
                          type: 'DelimitedTextWriteSettings'
                          quoteAllText: true
                          fileExtension: '.txt'
                        }
                      }
                      enableStaging: false
                      validateDataConsistency: true
                      translator: {
                        value: '@json(item().CopyActivitySettings).translator'
                        type: 'Expression'
                      }
                    }
                    inputs: [
                      {
                        referenceName: 'MetadataDrivenCopy_SQL_SourceDS'
                        type: 'DatasetReference'
                        parameters: {
                          cw_schema: {
                            value: '@json(item().SourceObjectSettings).schema'
                            type: 'Expression'
                          }
                          cw_table: {
                            value: '@json(item().SourceObjectSettings).table'
                            type: 'Expression'
                          }
                          cw_ls_serverName: {
                            value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).serverName'
                            type: 'Expression'
                          }
                          cw_ls_databaseName: {
                            value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).databaseName'
                            type: 'Expression'
                          }
                          cw_ls_userName: {
                            value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).userName'
                            type: 'Expression'
                          }
                          cw_ls_passwordSecretName: {
                            value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).passwordSecretName'
                            type: 'Expression'
                          }
                        }
                      }
                    ]
                    outputs: [
                      {
                        referenceName: 'MetadataDrivenCopy_SQL_DestinationDS'
                        type: 'DatasetReference'
                        parameters: {
                          cw_compressionCodec: {
                            value: '@json(item().SinkObjectSettings).compressionCodec'
                            type: 'Expression'
                          }
                          cw_compressionLevel: {
                            value: '@json(item().SinkObjectSettings).compressionLevel'
                            type: 'Expression'
                          }
                          cw_columnDelimiter: {
                            value: '@json(item().SinkObjectSettings).columnDelimiter'
                            type: 'Expression'
                          }
                          cw_escapeChar: {
                            value: '@json(item().SinkObjectSettings).escapeChar'
                            type: 'Expression'
                          }
                          cw_quoteChar: {
                            value: '@json(item().SinkObjectSettings).quoteChar'
                            type: 'Expression'
                          }
                          cw_firstRowAsHeader: {
                            value: '@json(item().SinkObjectSettings).firstRowAsHeader'
                            type: 'Expression'
                          }
                          cw_fileName: {
                            value: '@json(item().SinkObjectSettings).fileName'
                            type: 'Expression'
                          }
                          cw_folderPath: {
                            value: '@json(item().SinkObjectSettings).folderPath'
                            type: 'Expression'
                          }
                          cw_container: {
                            value: '@json(item().SinkObjectSettings).container'
                            type: 'Expression'
                          }
                          cw_pipelineStartTime: {
                            value: '@pipeline().parameters.PipelineStartTime'
                            type: 'Expression'
                          }
                        }
                      }
                    ]
                  }
                  {
                    name: 'DefaultLoadOneObject_Parquet'
                    description: 'Take a full snapshot on this object and copy it to the destination'
                    type: 'Copy'
                    dependsOn: [
                      {
                        activity: 'LogParquetCommence'
                        dependencyConditions: [
                          'Succeeded'
                        ]
                      }
                    ]
                    policy: {
                      timeout: '7.00:00:00'
                      retry: 1
                      retryIntervalInSeconds: 300
                      secureOutput: false
                      secureInput: false
                    }
                    userProperties: [
                      {
                        name: 'Source'
                        value: '@{json(item().ParquetSourceObjectSettings).fileSystem}/@{json(item().ParquetSourceObjectSettings).folderPath}/@{json(item().ParquetSourceObjectSettings).fileName}'
                      }
                      {
                        name: 'Destination'
                        value: '@{json(item().ParquetSinkObjectSettings).fileSystem}/@{json(item().ParquetSinkObjectSettings).folderPath}/@{json(item().ParquetSinkObjectSettings).fileName}'
                      }
                    ]
                    typeProperties: {
                      source: {
                        type: 'DelimitedTextSource'
                        additionalColumns: [
                          {
                            name: 'loadDate'
                            value: {
                              value: '@formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyyMMdd\')'
                              type: 'Expression'
                            }
                          }
                        ]
                        storeSettings: {
                          type: 'AzureBlobFSReadSettings'
                          recursive: {
                            value: '@json(item().ParquetCopySourceSettings).recursive'
                            type: 'Expression'
                          }
                        }
                        formatSettings: {
                          type: 'DelimitedTextReadSettings'
                          skipLineCount: {
                            value: '@json(item().ParquetCopySourceSettings).skipLineCount'
                            type: 'Expression'
                          }
                        }
                      }
                      sink: {
                        type: 'ParquetSink'
                        storeSettings: {
                          type: 'AzureBlobFSWriteSettings'
                        }
                        formatSettings: {
                          type: 'ParquetWriteSettings'
                        }
                      }
                      enableStaging: false
                      validateDataConsistency: true
                      translator: {
                        value: '@json(item().ParquetCopyActivitySettings).translator'
                        type: 'Expression'
                      }
                    }
                    inputs: [
                      {
                        referenceName: 'MetadataDrivenCopy_SQL_ParquetSourceDS'
                        type: 'DatasetReference'
                        parameters: {
                          cw_columnDelimiter: {
                            value: '@json(item().ParquetSourceObjectSettings).columnDelimiter'
                            type: 'Expression'
                          }
                          cw_escapeChar: {
                            value: '@json(item().ParquetSourceObjectSettings).escapeChar'
                            type: 'Expression'
                          }
                          cw_quoteChar: {
                            value: '@json(item().ParquetSourceObjectSettings).quoteChar'
                            type: 'Expression'
                          }
                          cw_firstRowAsHeader: {
                            value: '@json(item().ParquetSourceObjectSettings).firstRowAsHeader'
                            type: 'Expression'
                          }
                          cw_fileName: {
                            value: '@json(item().ParquetSourceObjectSettings).fileName'
                            type: 'Expression'
                          }
                          cw_folderPath: {
                            value: '@json(item().ParquetSourceObjectSettings).folderPath'
                            type: 'Expression'
                          }
                          cw_fileSystem: {
                            value: '@json(item().ParquetSourceObjectSettings).fileSystem'
                            type: 'Expression'
                          }
                          cw_pipelineStartTime: {
                            value: '@pipeline().parameters.PipelineStartTime'
                            type: 'Expression'
                          }
                        }
                      }
                    ]
                    outputs: [
                      {
                        referenceName: 'MetadataDrivenCopy_SQL_ParquetDestinationDS'
                        type: 'DatasetReference'
                        parameters: {
                          cw_compressionCodec: {
                            value: '@json(item().ParquetSinkObjectSettings).compressionCodec'
                            type: 'Expression'
                          }
                          cw_columnDelimiter: {
                            value: '@json(item().ParquetSinkObjectSettings).columnDelimiter'
                            type: 'Expression'
                          }
                          cw_escapeChar: {
                            value: '@json(item().ParquetSinkObjectSettings).escapeChar'
                            type: 'Expression'
                          }
                          cw_quoteChar: {
                            value: '@json(item().ParquetSinkObjectSettings).quoteChar'
                            type: 'Expression'
                          }
                          cw_fileName: {
                            value: '@json(item().ParquetSinkObjectSettings).fileName'
                            type: 'Expression'
                          }
                          cw_folderPath: {
                            value: '@json(item().ParquetSinkObjectSettings).folderPath'
                            type: 'Expression'
                          }
                          cw_fileSystem: {
                            value: '@json(item().ParquetSinkObjectSettings).fileSystem'
                            type: 'Expression'
                          }
                          cw_pipelineStartTime: {
                            value: '@pipeline().parameters.PipelineStartTime'
                            type: 'Expression'
                          }
                        }
                      }
                    ]
                  }
                  {
                    name: 'LogCSVCommence'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'csv copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: null
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).schema'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'commenced'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).table'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'LogCSVSuccess'
                    type: 'SqlServerStoredProcedure'
                    dependsOn: [
                      {
                        activity: 'DefaultFullLoadOneObject'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'csv copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: null
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).schema'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'succeeded'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).table'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                        rowCount: {
                          value: {
                            value: '@activity(\'DefaultFullLoadOneObject\').output?.rowsCopied'
                            type: 'Expression'
                          }
                          type: 'Int64'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'LogParquetCommence'
                    type: 'SqlServerStoredProcedure'
                    dependsOn: [
                      {
                        activity: 'LogCSVSuccess'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'parquet copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: null
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).schema'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'commenced'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).table'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'LogCSVFail'
                    type: 'SqlServerStoredProcedure'
                    dependsOn: [
                      {
                        activity: 'DefaultFullLoadOneObject'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'csv copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: {
                            value: '@activity(\'DefaultFullLoadOneObject\').error?.message'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).schema'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'failed'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).table'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'LogParquetSuccess'
                    type: 'SqlServerStoredProcedure'
                    dependsOn: [
                      {
                        activity: 'DefaultLoadOneObject_Parquet'
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
                      storedProcedureName: '[config].[sp_upd_AuditLog]'
                      storedProcedureParameters: {
                        activity: {
                          value: 'parquet copy'
                          type: 'String'
                        }
                        commenceDateTime: {
                          value: null
                          type: 'DateTime'
                        }
                        completeDateTime: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        elMethod: {
                          value: {
                            value: '@json(item().DataLoadingBehaviorSettings).dataLoadingBehavior'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        errorText: {
                          value: null
                          type: 'String'
                        }
                        runDate: {
                          value: {
                            value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                            type: 'Expression'
                          }
                          type: 'DateTime'
                        }
                        schedule: {
                          value: {
                            value: '@pipeline().parameters.TriggerName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        schema: {
                          value: {
                            value: '@json(item().SourceObjectSettings).schema'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        sourceType: {
                          value: {
                            value: '@item().SourceConnectionSettingsName'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        status: {
                          value: 'succeeded'
                          type: 'String'
                        }
                        table: {
                          value: {
                            value: '@json(item().SourceObjectSettings).table'
                            type: 'Expression'
                          }
                          type: 'String'
                        }
                        watermark: {
                          value: null
                          type: 'String'
                        }
                        rowsCopied: {
                          value: {
                            value: '@activity(\'DefaultLoadOneObject_Parquet\').output?.rowsCopied'
                            type: 'Expression'
                          }
                          type: 'Int64'
                        }
                      }
                    }
                    linkedServiceName: {
                      referenceName: 'ls_azsqldb_metadatacontroldb'
                      type: 'LinkedServiceReference'
                    }
                  }
                  {
                    name: 'Fail_LogCSVFail'
                    type: 'Fail'
                    dependsOn: [
                      {
                        activity: 'LogCSVFail'
                        dependencyConditions: [
                          'Completed'
                        ]
                      }
                    ]
                    userProperties: []
                    typeProperties: {
                      message: {
                        value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                        type: 'Expression'
                      }
                      errorCode: '3204'
                    }
                  }
                ]
              }
            }
            {
              name: 'GetSourceConnectionValues'
              type: 'Lookup'
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
                source: {
                  type: 'AzureSqlSource'
                  sqlReaderQuery: {
                    value: 'select ConnectionSettings from @{pipeline().parameters.ConnectionControlTableName} where Name = \'@{item().SourceConnectionSettingsName} \' AND Id = @{item().Id}'
                    type: 'Expression'
                  }
                  partitionOption: 'None'
                }
                dataset: {
                  referenceName: 'MetadataDrivenCopy_SQL_ControlDS'
                  type: 'DatasetReference'
                  parameters: {}
                }
                firstRowOnly: false
              }
            }
            {
              name: 'GetTableMetadataInfo'
              type: 'Lookup'
              dependsOn: [
                {
                  activity: 'GetSourceConnectionValues'
                  dependencyConditions: [
                    'Succeeded'
                  ]
                }
              ]
              policy: {
                timeout: '0.12:00:00'
                retry: 0
                retryIntervalInSeconds: 30
                secureOutput: false
                secureInput: false
              }
              userProperties: []
              typeProperties: {
                source: {
                  type: 'AzureSqlSource'
                  sqlReaderQuery: {
                    value: 'select (SELECT [TABLE_CATALOG]\n      ,[TABLE_SCHEMA]\n      ,[TABLE_NAME]\n      ,[COLUMN_NAME]\n      ,[ORDINAL_POSITION]\n      ,[DATA_TYPE]\n      ,[CHARACTER_MAXIMUM_LENGTH]\nFROM config.@{json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).databaseName} \nwhere TABLE_NAME = \'@{json(item().SourceObjectSettings).table}\' \nand TABLE_SCHEMA = \'@{json(item().SourceObjectSettings).schema}\' \nFOR JSON AUTO) as json_string'
                    type: 'Expression'
                  }
                  queryTimeout: '02:00:00'
                  partitionOption: 'None'
                }
                dataset: {
                  referenceName: 'MetadataDrivenCopy_SQL_ControlDS'
                  type: 'DatasetReference'
                  parameters: {}
                }
                firstRowOnly: false
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
      ObjectsPerGroupToCopy: {
        type: 'Array'
      }
      ConnectionControlTableName: {
        type: 'String'
      }
      TriggerName: {
        type: 'string'
      }
      PipelineStartTime: {
        type: 'string'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_SQL'
    }
    annotations: []
    lastPublishTime: '2022-05-23T04:33:48Z'
  }
  dependsOn: [
    '${factoryId}/datasets/MetadataDrivenCopy_SQL_ControlDS'
    '${factoryId}/datasets/MetadataDrivenCopy_SQL_SourceDS'
    '${factoryId}/datasets/MetadataDrivenCopy_SQL_DestinationDS'
    '${factoryId}/datasets/MetadataDrivenCopy_SQL_ParquetSourceDS'
    '${factoryId}/datasets/MetadataDrivenCopy_SQL_ParquetDestinationDS'
    '${factoryId}/linkedServices/ls_azsqldb_metadatacontroldb'
    '${factoryId}/linkedServices/ls_synapsesqlondemand_gen01'
  ]
}

resource factoryName_MetadataDrivenCopy_SQL_MiddleLevel 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_SQL_MiddleLevel'
  properties: {
    description: 'This pipeline will copy one batch of objects. The objects belonging to this batch will be copied parallelly.'
    activities: [
      {
        name: 'DivideOneBatchIntoMultipleGroups'
        description: 'Divide objects from single batch into multiple sub parallel groups to avoid reaching the output limit of lookup activity.'
        type: 'ForEach'
        dependsOn: []
        userProperties: []
        typeProperties: {
          items: {
            value: '@range(0, add(div(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity),\n                    if(equals(mod(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity), 0), 0, 1)))'
            type: 'Expression'
          }
          isSequential: false
          batchCount: 50
          activities: [
            {
              name: 'GetObjectsPerGroupToCopy'
              description: 'Get objects (tables etc.) from control table required to be copied in this group. The order of objects to be copied following the TaskId in control table (ORDER BY [TaskId] DESC).'
              type: 'Lookup'
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
                source: {
                  type: 'AzureSqlSource'
                  sqlReaderQuery: {
                    value: 'WITH OrderedControlTable AS (\n                             SELECT *, ROW_NUMBER() OVER (ORDER BY [TaskId], [Id] DESC) AS RowNumber\n                             FROM @{pipeline().parameters.MainControlTableName}\n                             where TopLevelPipelineName = \'@{pipeline().parameters.TopLevelPipelineName}\'\n                             and TriggerName like \'%@{pipeline().parameters.TriggerName}%\' and CopyEnabled = 1)\n                             SELECT * FROM OrderedControlTable WHERE RowNumber BETWEEN @{add(mul(int(item()),pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity),\n                             add(mul(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.CurrentSequentialNumberOfBatch), 1))}\n                             AND @{min(add(mul(int(item()), pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity), add(mul(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.CurrentSequentialNumberOfBatch),\n                             pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity)),\n                            mul(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, add(pipeline().parameters.CurrentSequentialNumberOfBatch,1)), pipeline().parameters.SumOfObjectsToCopy)}'
                    type: 'Expression'
                  }
                  partitionOption: 'None'
                }
                dataset: {
                  referenceName: 'MetadataDrivenCopy_SQL_ControlDS'
                  type: 'DatasetReference'
                  parameters: {}
                }
                firstRowOnly: false
              }
            }
            {
              name: 'CopyObjectsInOneGroup'
              description: 'Execute another pipeline to copy objects from one group. The objects belonging to this group will be copied parallelly.'
              type: 'ExecutePipeline'
              dependsOn: [
                {
                  activity: 'GetObjectsPerGroupToCopy'
                  dependencyConditions: [
                    'Succeeded'
                  ]
                }
              ]
              userProperties: []
              typeProperties: {
                pipeline: {
                  referenceName: 'MetadataDrivenCopy_SQL_BottomLevel'
                  type: 'PipelineReference'
                }
                waitOnCompletion: true
                parameters: {
                  ObjectsPerGroupToCopy: {
                    value: '@activity(\'GetObjectsPerGroupToCopy\').output.value'
                    type: 'Expression'
                  }
                  ConnectionControlTableName: {
                    value: '@pipeline().parameters.ConnectionControlTableName'
                    type: 'Expression'
                  }
                  TriggerName: {
                    value: '@pipeline().parameters.TriggerName'
                    type: 'Expression'
                  }
                  PipelineStartTime: {
                    value: '@pipeline().parameters.PipelineStartTime'
                    type: 'Expression'
                  }
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
      MaxNumberOfObjectsReturnedFromLookupActivity: {
        type: 'Int'
      }
      TopLevelPipelineName: {
        type: 'String'
      }
      TriggerName: {
        type: 'String'
      }
      CurrentSequentialNumberOfBatch: {
        type: 'Int'
      }
      SumOfObjectsToCopy: {
        type: 'Int'
      }
      SumOfObjectsToCopyForCurrentBatch: {
        type: 'Int'
      }
      MainControlTableName: {
        type: 'String'
      }
      ConnectionControlTableName: {
        type: 'String'
      }
      PipelineStartTime: {
        type: 'string'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_SQL'
    }
    annotations: []
    lastPublishTime: '2022-05-16T01:05:15Z'
  }
  dependsOn: [
    '${factoryId}/datasets/MetadataDrivenCopy_SQL_ControlDS'
    '${factoryId}/pipelines/MetadataDrivenCopy_SQL_BottomLevel'
  ]
}

resource factoryName_MetadataDrivenCopy_SQL_TopLevel 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_SQL_TopLevel'
  properties: {
    description: 'This pipeline will count the total number of objects (tables etc.) required to be copied in this run, come up with the number of sequential batches based on the max allowed concurrent copy task, and then execute another pipeline to copy different batches sequentially.'
    activities: [
      {
        name: 'GetSumOfObjectsToCopy'
        description: 'Count the total number of objects (tables etc.) required to be copied in this run.'
        type: 'Lookup'
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
          source: {
            type: 'AzureSqlSource'
            sqlReaderQuery: {
              value: 'SELECT count(*) as count FROM @{pipeline().parameters.MainControlTableName} where TopLevelPipelineName=\'@{pipeline().Pipeline}\' and TriggerName like \'%@{pipeline().parameters.trigger}%\' and CopyEnabled = 1'
              type: 'Expression'
            }
            partitionOption: 'None'
          }
          dataset: {
            referenceName: 'MetadataDrivenCopy_SQL_ControlDS'
            type: 'DatasetReference'
            parameters: {}
          }
        }
      }
      {
        name: 'CopyBatchesOfObjectsSequentially'
        description: 'Come up with the number of sequential batches based on the max allowed concurrent copy tasks, and then execute another pipeline to copy different batches sequentially.'
        type: 'ForEach'
        dependsOn: [
          {
            activity: 'GetSumOfObjectsToCopy'
            dependencyConditions: [
              'Succeeded'
            ]
          }
        ]
        userProperties: []
        typeProperties: {
          items: {
            value: '@range(0, add(div(activity(\'GetSumOfObjectsToCopy\').output.firstRow.count,\n                    pipeline().parameters.MaxNumberOfConcurrentTasks),\n                    if(equals(mod(activity(\'GetSumOfObjectsToCopy\').output.firstRow.count,\n                    pipeline().parameters.MaxNumberOfConcurrentTasks), 0), 0, 1)))'
            type: 'Expression'
          }
          isSequential: true
          activities: [
            {
              name: 'CopyObjectsInOneBatch'
              description: 'Execute another pipeline to copy one batch of objects. The objects belonging to this batch will be copied parallelly.'
              type: 'ExecutePipeline'
              dependsOn: []
              userProperties: []
              typeProperties: {
                pipeline: {
                  referenceName: 'MetadataDrivenCopy_SQL_MiddleLevel'
                  type: 'PipelineReference'
                }
                waitOnCompletion: true
                parameters: {
                  MaxNumberOfObjectsReturnedFromLookupActivity: {
                    value: '@pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity'
                    type: 'Expression'
                  }
                  TopLevelPipelineName: {
                    value: '@{pipeline().Pipeline}'
                    type: 'Expression'
                  }
                  TriggerName: {
                    value: '@pipeline().parameters.trigger'
                    type: 'Expression'
                  }
                  CurrentSequentialNumberOfBatch: {
                    value: '@item()'
                    type: 'Expression'
                  }
                  SumOfObjectsToCopy: {
                    value: '@activity(\'GetSumOfObjectsToCopy\').output.firstRow.count'
                    type: 'Expression'
                  }
                  SumOfObjectsToCopyForCurrentBatch: {
                    value: '@min(pipeline().parameters.MaxNumberOfConcurrentTasks, activity(\'GetSumOfObjectsToCopy\').output.firstRow.count)'
                    type: 'Expression'
                  }
                  MainControlTableName: {
                    value: '@pipeline().parameters.MainControlTableName'
                    type: 'Expression'
                  }
                  ConnectionControlTableName: {
                    value: '@pipeline().parameters.ConnectionControlTableName'
                    type: 'Expression'
                  }
                  PipelineStartTime: {
                    value: '@pipeline().parameters.PipelineStartTime'
                    type: 'Expression'
                  }
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
      MaxNumberOfObjectsReturnedFromLookupActivity: {
        type: 'Int'
        defaultValue: 5000
      }
      MaxNumberOfConcurrentTasks: {
        type: 'Int'
        defaultValue: 16
      }
      MainControlTableName: {
        type: 'String'
        defaultValue: 'config.SQLControlTable'
      }
      ConnectionControlTableName: {
        type: 'String'
        defaultValue: 'config.SQLConnectionControlTable'
      }
      trigger: {
        type: 'string'
        defaultValue: 'Manual'
      }
      PipelineStartTime: {
        type: 'string'
      }
    }
    folder: {
      name: 'MetadataDrivenCopy_SQL'
    }
    annotations: [
      'MetadataDrivenSolution'
    ]
    lastPublishTime: '2022-05-23T03:02:47Z'
  }
  dependsOn: [
    '${factoryId}/datasets/MetadataDrivenCopy_SQL_ControlDS'
    '${factoryId}/pipelines/MetadataDrivenCopy_SQL_MiddleLevel'
  ]
}

resource factoryName_Schema_Middle_Level 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/Schema_Middle_Level'
  properties: {
    description: 'This pipeline will execute the pipeline appropriate for each incoming source type. The objects belonging to this batch will be copied parallelly.'
    activities: [
      {
        name: 'Switch_SystemType'
        description: 'Switch for each source type {oracle, sql, flat file}'
        type: 'Switch'
        dependsOn: []
        userProperties: []
        typeProperties: {
          on: {
            value: '@pipeline().parameters.System'
            type: 'Expression'
          }
          cases: [
            {
              value: 'SQL'
              activities: [
                {
                  name: 'Execute SQL'
                  description: 'Execute the Bottom Level pipeline (Schema_SQL_Bottom_Level) for the parsed source.'
                  type: 'ExecutePipeline'
                  dependsOn: []
                  userProperties: []
                  typeProperties: {
                    pipeline: {
                      referenceName: 'Schema_SQL_Bottom_Level'
                      type: 'PipelineReference'
                    }
                    waitOnCompletion: true
                    parameters: {
                      ConnectionControlTableName: '[config].[SQLConnectionControlTable]'
                      SourceName: {
                        value: '@pipeline().parameters.Source'
                        type: 'Expression'
                      }
                    }
                  }
                }
              ]
            }
            {
              value: 'Oracle'
              activities: [
                {
                  name: 'Execute Oracle'
                  description: 'Execute the Bottom Level pipeline (Schema_Oracle_Bottom_Level) for the parsed source.'
                  type: 'ExecutePipeline'
                  dependsOn: []
                  userProperties: []
                  typeProperties: {
                    pipeline: {
                      referenceName: 'Schema_Oracle_Bottom_Level'
                      type: 'PipelineReference'
                    }
                    waitOnCompletion: true
                    parameters: {
                      ConnectionControlTableName: '[config].[ConnectionReference]'
                      SourceName: {
                        value: '@pipeline().parameters.Source'
                        type: 'Expression'
                      }
                    }
                  }
                }
              ]
            }
            {
              value: 'Flat'
              activities: [
                {
                  name: 'Wait_flat'
                  description: 'Execute a wait only, where the source type is Oracle (Placeholder).'
                  type: 'Wait'
                  dependsOn: []
                  userProperties: []
                  typeProperties: {
                    waitTimeInSeconds: 1
                  }
                }
              ]
            }
          ]
          defaultActivities: [
            {
              name: 'Wait_default'
              description: 'Execute a wait only, where the source type is not provided.'
              type: 'Wait'
              dependsOn: []
              userProperties: []
              typeProperties: {
                waitTimeInSeconds: 1
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
      System: {
        type: 'string'
      }
      Source: {
        type: 'string'
      }
    }
    folder: {
      name: 'Metadata/Schema'
    }
    annotations: []
  }
  dependsOn: [
    '${factoryId}/pipelines/Schema_SQL_Bottom_Level'
    '${factoryId}/pipelines/Schema_Oracle_Bottom_Level'
  ]
}

resource factoryName_Schema_Oracle_Bottom_Level 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/Schema_Oracle_Bottom_Level'
  properties: {
    description: 'For each source connection, update the information schema stored in the control database, and then set the mappings for the control items associated with that database.'
    activities: [
      {
        name: 'GetSourceConnectionValues'
        description: 'Retrieve the source connections for the parsed source.'
        type: 'Lookup'
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
          source: {
            type: 'AzureSqlSource'
            sqlReaderQuery: {
              value: 'SELECT top 1 [ConnectionSettings]\n  FROM @{pipeline().parameters.ConnectionControlTableName}\n where JSON_VALUE([ConnectionSettings], \'$.databaseName\') = \'@{pipeline().parameters.SourceName}\''
              type: 'Expression'
            }
            queryTimeout: '02:00:00'
            partitionOption: 'None'
          }
          dataset: {
            referenceName: 'MetadataDrivenCopy_Oracle_ControlDS'
            type: 'DatasetReference'
            parameters: {}
          }
          firstRowOnly: false
        }
      }
      {
        name: 'CopyMetadata'
        description: 'Copy the information schema information, to a control object relevant for each source.'
        type: 'Copy'
        dependsOn: [
          {
            activity: 'GetSourceConnectionValues'
            dependencyConditions: [
              'Succeeded'
            ]
          }
        ]
        policy: {
          timeout: '7.00:00:00'
          retry: 1
          retryIntervalInSeconds: 60
          secureOutput: false
          secureInput: false
        }
        userProperties: []
        typeProperties: {
          source: {
            type: 'OracleSource'
            oracleReaderQuery: {
              value: 'select * from ALL_TAB_COLUMNS where OWNER = \'@{json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).user}\''
              type: 'Expression'
            }
            partitionOption: 'None'
            convertDecimalToInteger: false
            queryTimeout: '02:00:00'
          }
          sink: {
            type: 'AzureSqlSink'
            preCopyScript: {
              value: '@{concat(\'IF OBJECT_ID(\'\'config.\',pipeline().parameters.SourceName,\'\'\', \'\'U\'\') IS NOT NULL \',\n   \'DROP TABLE config.\', pipeline().parameters.SourceName,\';\')}'
              type: 'Expression'
            }
            writeBehavior: 'insert'
            sqlWriterUseTableLock: true
            tableOption: 'autoCreate'
            disableMetricsCollection: false
          }
          enableStaging: false
          translator: {
            type: 'TabularTranslator'
            typeConversion: true
            typeConversionSettings: {
              allowDataTruncation: true
              treatBooleanAsNumber: false
            }
          }
        }
        inputs: [
          {
            referenceName: 'MetadataDrivenCopy_Oracle_SourceDS'
            type: 'DatasetReference'
            parameters: {
              cw_schema: 'cw_schema'
              cw_table: 'ALL_TAB_COLUMNS'
              cw_ls_host: {
                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).host'
                type: 'Expression'
              }
              cw_ls_port: {
                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).port'
                type: 'Expression'
              }
              cw_ls_SID: {
                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).SID'
                type: 'Expression'
              }
              cw_ls_userName: {
                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).userName'
                type: 'Expression'
              }
              cw_ls_passwordSecretName: {
                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).passwordSecretName'
                type: 'Expression'
              }
            }
          }
        ]
        outputs: [
          {
            referenceName: 'MetadataDrivenCopy_Oracle_Schema'
            type: 'DatasetReference'
            parameters: {
              System: {
                value: '@pipeline().parameters.SourceName'
                type: 'Expression'
              }
            }
          }
        ]
      }
      {
        name: 'SetMappings'
        description: 'Update the mappings based on the retrieved metadata, for each control item associated with that database.'
        type: 'SqlServerStoredProcedure'
        dependsOn: [
          {
            activity: 'CopyMetadata'
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
          storedProcedureName: '[config].[sp_upd_MapOracleDataTypes]'
          storedProcedureParameters: {
            system: {
              value: {
                value: '@pipeline().parameters.SourceName'
                type: 'Expression'
              }
              type: 'String'
            }
          }
        }
        linkedServiceName: {
          referenceName: 'ls_azsqldb_metadatacontroldb'
          type: 'LinkedServiceReference'
        }
      }
    ]
    policy: {
      elapsedTimeMetric: {}
    }
    parameters: {
      ConnectionControlTableName: {
        type: 'string'
      }
      SourceName: {
        type: 'string'
      }
    }
    folder: {
      name: 'Metadata/Schema'
    }
    annotations: []
  }
  dependsOn: [
    '${factoryId}/datasets/MetadataDrivenCopy_Oracle_ControlDS'
    '${factoryId}/datasets/MetadataDrivenCopy_Oracle_SourceDS'
    '${factoryId}/datasets/MetadataDrivenCopy_Oracle_Schema'
    '${factoryId}/linkedServices/ls_azsqldb_metadatacontroldb'
  ]
}

resource factoryName_Schema_SQL_Bottom_Level 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/Schema_SQL_Bottom_Level'
  properties: {
    description: 'For each source connection, update the information schema stored in the control database, and then set the mappings for the control items associated with that database.'
    activities: [
      {
        name: 'GetSourceConnectionValues'
        description: 'Retrieve the source connections for the parsed source.'
        type: 'Lookup'
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
          source: {
            type: 'AzureSqlSource'
            sqlReaderQuery: {
              value: 'SELECT top 1 [ConnectionSettings]\n  FROM @{pipeline().parameters.ConnectionControlTableName}\nwhere JSON_VALUE([ConnectionSettings], \'$.databaseName\') = \'@{pipeline().parameters.SourceName}\''
              type: 'Expression'
            }
            queryTimeout: '02:00:00'
            partitionOption: 'None'
          }
          dataset: {
            referenceName: 'MetadataDrivenCopy_SQL_ControlDS'
            type: 'DatasetReference'
            parameters: {}
          }
          firstRowOnly: false
        }
      }
      {
        name: 'CopyMetadata'
        description: 'Copy the information schema information, to a control object relevant for each source.'
        type: 'Copy'
        dependsOn: [
          {
            activity: 'GetSourceConnectionValues'
            dependencyConditions: [
              'Succeeded'
            ]
          }
        ]
        policy: {
          timeout: '7.00:00:00'
          retry: 1
          retryIntervalInSeconds: 60
          secureOutput: false
          secureInput: false
        }
        userProperties: []
        typeProperties: {
          source: {
            type: 'SqlServerSource'
            queryTimeout: '02:00:00'
            partitionOption: 'None'
          }
          sink: {
            type: 'AzureSqlSink'
            preCopyScript: {
              value: '@{concat(\'IF OBJECT_ID(\'\'config.\',pipeline().parameters.SourceName,\'\'\', \'\'U\'\') IS NOT NULL \',\n   \'DROP TABLE config.\', pipeline().parameters.SourceName,\';\')}'
              type: 'Expression'
            }
            writeBehavior: 'insert'
            sqlWriterUseTableLock: true
            tableOption: 'autoCreate'
            disableMetricsCollection: false
          }
          enableStaging: false
          translator: {
            type: 'TabularTranslator'
            typeConversion: true
            typeConversionSettings: {
              allowDataTruncation: true
              treatBooleanAsNumber: false
            }
          }
        }
        inputs: [
          {
            referenceName: 'MetadataDrivenCopy_SQL_SourceDS'
            type: 'DatasetReference'
            parameters: {
              cw_schema: 'INFORMATION_SCHEMA'
              cw_table: 'COLUMNS'
              cw_ls_serverName: {
                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).serverName'
                type: 'Expression'
              }
              cw_ls_databaseName: {
                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).databaseName'
                type: 'Expression'
              }
              cw_ls_userName: {
                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).userName'
                type: 'Expression'
              }
              cw_ls_passwordSecretName: {
                value: '@json(activity(\'GetSourceConnectionValues\').output.value[0].ConnectionSettings).passwordSecretName'
                type: 'Expression'
              }
            }
          }
        ]
        outputs: [
          {
            referenceName: 'MetadataDrivenCopy_SQL_Schema'
            type: 'DatasetReference'
            parameters: {
              System: {
                value: '@pipeline().parameters.SourceName'
                type: 'Expression'
              }
            }
          }
        ]
      }
      {
        name: 'SetMappings'
        description: 'Update the mappings based on the retrieved metadata, for each control item associated with that database.'
        type: 'SqlServerStoredProcedure'
        dependsOn: [
          {
            activity: 'CopyMetadata'
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
          storedProcedureName: '[config].[sp_upd_MapSQLDataTypes]'
          storedProcedureParameters: {
            system: {
              value: {
                value: '@pipeline().parameters.SourceName'
                type: 'Expression'
              }
              type: 'String'
            }
          }
        }
        linkedServiceName: {
          referenceName: 'ls_azsqldb_metadatacontroldb'
          type: 'LinkedServiceReference'
        }
      }
    ]
    policy: {
      elapsedTimeMetric: {}
    }
    parameters: {
      ConnectionControlTableName: {
        type: 'string'
      }
      SourceName: {
        type: 'string'
      }
    }
    folder: {
      name: 'Metadata/Schema'
    }
    annotations: []
  }
  dependsOn: [
    '${factoryId}/datasets/MetadataDrivenCopy_SQL_ControlDS'
    '${factoryId}/datasets/MetadataDrivenCopy_SQL_SourceDS'
    '${factoryId}/datasets/MetadataDrivenCopy_SQL_Schema'
    '${factoryId}/linkedServices/ls_azsqldb_metadatacontroldb'
  ]
}

resource factoryName_Schema_Top_Level 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/Schema_Top_Level'
  properties: {
    description: 'This pipeline retrieves a list of connections referenced within the control database, and executes a number of batches based through  execution of another pipeline to copy metadata information about each source.'
    activities: [
      {
        name: 'LookupConnections'
        description: 'Looks up the connection details listed in the Control database.'
        type: 'Lookup'
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
          source: {
            type: 'AzureSqlSource'
            sqlReaderQuery: '/****** Script for SelectTopNRows command from SSMS  ******/\nSELECT [System]=LTRIM(RTRIM([System]))\n      ,[ShortName]=LTRIM(RTRIM([ShortName]))\n  FROM [config].[ConnectionReference]'
            queryTimeout: '02:00:00'
            partitionOption: 'None'
          }
          dataset: {
            referenceName: 'MetadataDrivenCopy_SQL_ControlDS'
            type: 'DatasetReference'
            parameters: {}
          }
          firstRowOnly: false
        }
      }
      {
        name: 'ForEachConnection'
        description: 'Iterates through each connection listed within the control database.'
        type: 'ForEach'
        dependsOn: [
          {
            activity: 'LookupConnections'
            dependencyConditions: [
              'Succeeded'
            ]
          }
        ]
        userProperties: []
        typeProperties: {
          items: {
            value: '@activity(\'LookupConnections\').output.value'
            type: 'Expression'
          }
          isSequential: false
          activities: [
            {
              name: 'Execute Middle Level'
              description: 'Executes another pipeline to generate metadata relating to the sources identified within the control database.'
              type: 'ExecutePipeline'
              dependsOn: []
              userProperties: []
              typeProperties: {
                pipeline: {
                  referenceName: 'Schema_Middle_Level'
                  type: 'PipelineReference'
                }
                waitOnCompletion: true
                parameters: {
                  System: {
                    value: '@item().System'
                    type: 'Expression'
                  }
                  Source: {
                    value: '@item().ShortName'
                    type: 'Expression'
                  }
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
    folder: {
      name: 'Metadata/Schema'
    }
    annotations: []
  }
  dependsOn: [
    '${factoryId}/datasets/MetadataDrivenCopy_SQL_ControlDS'
    '${factoryId}/pipelines/Schema_Middle_Level'
  ]
}

resource factoryName_Transformation_BottomLevel 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/Transformation_BottomLevel'
  properties: {
    activities: [
      {
        name: 'ListObjectsFromOneGroup'
        description: 'List objects from one group and iterate each of them to downstream activities'
        type: 'ForEach'
        dependsOn: []
        userProperties: []
        typeProperties: {
          items: {
            value: '@pipeline().parameters.ObjectsPerGroupToCopy'
            type: 'Expression'
          }
          batchCount: 50
          activities: [
            {
              name: 'RouteJobsBasedOnLoadingBehavior'
              description: 'Check the loading behavior for each object if it requires full load or incremental load. If it is Default or FullLoad case, do full load. If it is DeltaLoad case, do incremental load.'
              type: 'Switch'
              dependsOn: []
              userProperties: []
              typeProperties: {
                on: {
                  value: '@item().DataLoadingBehaviorSettings'
                  type: 'Expression'
                }
                cases: [
                  {
                    value: 'SCD1_View'
                    activities: [
                      {
                        name: 'LogTransformCommence_FL'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'MDF_SCD1'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: 'SCD1'
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: null
                              type: 'Int64'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: 'view'
                              type: 'String'
                            }
                            status: {
                              value: 'commenced'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'MDF_SCD1'
                        type: 'ExecutePipeline'
                        dependsOn: [
                          {
                            activity: 'LogTransformCommence_FL'
                            dependencyConditions: [
                              'Succeeded'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          pipeline: {
                            referenceName: 'MDF_SCD1_VW'
                            type: 'PipelineReference'
                          }
                          waitOnCompletion: true
                          parameters: {
                            targetName: {
                              value: '@json(item().SinkObjectSettings).table'
                              type: 'Expression'
                            }
                            targetSchema: {
                              value: '@json(item().SinkObjectSettings).schema'
                              type: 'Expression'
                            }
                            sourceName: {
                              value: '@json(item().SourceObjectSettings).table'
                              type: 'Expression'
                            }
                            sourceSchema: {
                              value: '@json(item().SourceObjectSettings).schema'
                              type: 'Expression'
                            }
                            database: {
                              value: '@pipeline().parameters.databaseName'
                              type: 'Expression'
                            }
                          }
                        }
                      }
                      {
                        name: 'LogTransformSuccess_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'MDF_SCD1'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'MDF_SCD1'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: 'SCD1'
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: {
                                value: '@activity(\'MDF_SCD1\').output?.rowsCopied'
                                type: 'Expression'
                              }
                              type: 'Int64'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: 'view'
                              type: 'String'
                            }
                            status: {
                              value: 'succeeded'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogTransformFailure_FL'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'MDF_SCD1'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'MDF_SCD1'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: 'SCD1'
                              type: 'String'
                            }
                            errorText: {
                              value: {
                                value: '@activity(\'MDF_SCD1\').error?.message'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            rowsCopied: {
                              value: null
                              type: 'Int64'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: 'view'
                              type: 'String'
                            }
                            status: {
                              value: 'failed'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'Fail_LogTransformFail_FL'
                        type: 'Fail'
                        dependsOn: [
                          {
                            activity: 'LogTransformFailure_FL'
                            dependencyConditions: [
                              'Completed'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          message: {
                            value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                            type: 'Expression'
                          }
                          errorCode: '3204'
                        }
                      }
                    ]
                  }
                  {
                    value: 'SCD1_Procedure'
                    activities: [
                      {
                        name: 'LogTransformCommence_FL_SP'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'MDF_SCD1'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: 'SCD1'
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: null
                              type: 'Int64'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: 'view'
                              type: 'String'
                            }
                            status: {
                              value: 'commenced'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'MDF_SCD1_SP'
                        type: 'ExecutePipeline'
                        dependsOn: [
                          {
                            activity: 'LogTransformCommence_FL_SP'
                            dependencyConditions: [
                              'Succeeded'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          pipeline: {
                            referenceName: 'MDF_SCD1_SP'
                            type: 'PipelineReference'
                          }
                          waitOnCompletion: true
                          parameters: {
                            targetName: {
                              value: '@json(item().SinkObjectSettings).table'
                              type: 'Expression'
                            }
                            targetSchema: {
                              value: '@json(item().SinkObjectSettings).schema'
                              type: 'Expression'
                            }
                            sourceName: {
                              value: '@json(item().SourceObjectSettings).table'
                              type: 'Expression'
                            }
                            sourceSchema: {
                              value: '@json(item().SourceObjectSettings).schema'
                              type: 'Expression'
                            }
                            database: {
                              value: '@pipeline().parameters.databaseName'
                              type: 'Expression'
                            }
                          }
                        }
                      }
                      {
                        name: 'LogTransformSuccess_FL_SP'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'MDF_SCD1_SP'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'MDF_SCD1'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: 'SCD1'
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: {
                                value: '@activity(\'MDF_SCD1_SP\').output?.rowsCopied'
                                type: 'Expression'
                              }
                              type: 'Int64'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: 'view'
                              type: 'String'
                            }
                            status: {
                              value: 'succeeded'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogTransformFailure_FL_SP'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'MDF_SCD1_SP'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'MDF_SCD1'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: 'SCD1'
                              type: 'String'
                            }
                            errorText: {
                              value: {
                                value: '@activity(\'MDF_SCD1_SP\').error?.message'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            rowsCopied: {
                              value: null
                              type: 'Int64'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: 'view'
                              type: 'String'
                            }
                            status: {
                              value: 'failed'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'Fail_LogTransformFail_FL_SP'
                        type: 'Fail'
                        dependsOn: [
                          {
                            activity: 'LogTransformFailure_FL_SP'
                            dependencyConditions: [
                              'Completed'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          message: {
                            value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                            type: 'Expression'
                          }
                          errorCode: '3204'
                        }
                      }
                    ]
                  }
                  {
                    value: 'CETAS'
                    activities: [
                      {
                        name: 'LogTransformCommence_FL_CETAS'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'Snapshot'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: 'CETAS'
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: null
                              type: 'Int64'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: 'view'
                              type: 'String'
                            }
                            status: {
                              value: 'commenced'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'MDF_SCD1_CETAS'
                        type: 'ExecutePipeline'
                        dependsOn: [
                          {
                            activity: 'LogTransformCommence_FL_CETAS'
                            dependencyConditions: [
                              'Succeeded'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          pipeline: {
                            referenceName: 'ADF_CETAS_SP'
                            type: 'PipelineReference'
                          }
                          waitOnCompletion: true
                          parameters: {
                            targetName: {
                              value: '@json(item().SinkObjectSettings).table'
                              type: 'Expression'
                            }
                            targetSchema: {
                              value: '@json(item().SinkObjectSettings).schema'
                              type: 'Expression'
                            }
                            sourceName: {
                              value: '@json(item().SourceObjectSettings).table'
                              type: 'Expression'
                            }
                            sourceSchema: {
                              value: '@json(item().SourceObjectSettings).schema'
                              type: 'Expression'
                            }
                            container: {
                              value: '@item().targetContainer'
                              type: 'Expression'
                            }
                            storageAccount: {
                              value: '@json(item().SourceObjectSettings).storageAccount'
                              type: 'Expression'
                            }
                            PipelineStartTime: {
                              value: '@pipeline().parameters.PipelineStartTime'
                              type: 'Expression'
                            }
                          }
                        }
                      }
                      {
                        name: 'LogTransformSuccess_FL_CETAS'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'MDF_SCD1_CETAS'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'Snapshot'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: 'CETAS'
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: {
                                value: '@activity(\'MDF_SCD1_CETAS\').output?.rowsCopied'
                                type: 'Expression'
                              }
                              type: 'Int64'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: 'view'
                              type: 'String'
                            }
                            status: {
                              value: 'succeeded'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogTransformFailure_FL_SP_CETAS'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'MDF_SCD1_CETAS'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'Snapshot'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: 'CETAS'
                              type: 'String'
                            }
                            errorText: {
                              value: {
                                value: '@activity(\'MDF_SCD1_CETAS\').error?.message'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            rowsCopied: {
                              value: null
                              type: 'Int64'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: 'view'
                              type: 'String'
                            }
                            status: {
                              value: 'failed'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'Fail_LogTransformFail_FL_CETAS'
                        type: 'Fail'
                        dependsOn: [
                          {
                            activity: 'LogTransformFailure_FL_SP_CETAS'
                            dependencyConditions: [
                              'Completed'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          message: {
                            value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                            type: 'Expression'
                          }
                          errorCode: '3204'
                        }
                      }
                    ]
                  }
                  {
                    value: 'SCD2_View'
                    activities: [
                      {
                        name: 'LogTransformCommence_FL_2'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'MDF_SCD2'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: 'SCD2'
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: null
                              type: 'Int64'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: 'view'
                              type: 'String'
                            }
                            status: {
                              value: 'commenced'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'MDF_SCD2'
                        type: 'ExecutePipeline'
                        dependsOn: [
                          {
                            activity: 'LogTransformCommence_FL_2'
                            dependencyConditions: [
                              'Succeeded'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          pipeline: {
                            referenceName: 'MDF_SCD2_VW'
                            type: 'PipelineReference'
                          }
                          waitOnCompletion: true
                          parameters: {
                            targetName: {
                              value: '@json(item().SinkObjectSettings).table'
                              type: 'Expression'
                            }
                            targetSchema: {
                              value: '@json(item().SinkObjectSettings).schema'
                              type: 'Expression'
                            }
                            sourceName: {
                              value: '@json(item().SourceObjectSettings).table'
                              type: 'Expression'
                            }
                            sourceSchema: {
                              value: '@json(item().SourceObjectSettings).schema'
                              type: 'Expression'
                            }
                            database: {
                              value: '@pipeline().parameters.databaseName'
                              type: 'Expression'
                            }
                          }
                        }
                      }
                      {
                        name: 'LogTransformSuccess_FL_2'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'MDF_SCD2'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'MDF_SCD2'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: 'SCD2'
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: {
                                value: '@activity(\'MDF_SCD2\').output?.rowsCopied'
                                type: 'Expression'
                              }
                              type: 'Int64'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: 'view'
                              type: 'String'
                            }
                            status: {
                              value: 'succeeded'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'LogTransformFailure_FL_2'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'MDF_SCD2'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'MDF_SCD2'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: 'SCD2'
                              type: 'String'
                            }
                            errorText: {
                              value: {
                                value: '@activity(\'MDF_SCD2\').error?.message'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            rowsCopied: {
                              value: null
                              type: 'Int64'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: 'view'
                              type: 'String'
                            }
                            status: {
                              value: 'failed'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'Fail_LogTransformFail_FL_2'
                        type: 'Fail'
                        dependsOn: [
                          {
                            activity: 'LogTransformFailure_FL_2'
                            dependencyConditions: [
                              'Completed'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          message: {
                            value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                            type: 'Expression'
                          }
                          errorCode: '3204'
                        }
                      }
                    ]
                  }
                  {
                    value: 'EXTPQ'
                    activities: [
                      {
                        name: 'LogTransformCommence_FL_EXTPQ'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'Snapshot'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: 'EXTPQ'
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: null
                              type: 'Int64'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: 'view'
                              type: 'String'
                            }
                            status: {
                              value: 'commenced'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'MDF_SCD1_EXTPQ'
                        type: 'ExecutePipeline'
                        dependsOn: [
                          {
                            activity: 'LogTransformCommence_FL_EXTPQ'
                            dependencyConditions: [
                              'Succeeded'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          pipeline: {
                            referenceName: 'ADF_EXTPQ_TF'
                            type: 'PipelineReference'
                          }
                          waitOnCompletion: true
                          parameters: {
                            targetName: {
                              value: '@json(item().SinkObjectSettings).table'
                              type: 'Expression'
                            }
                            targetSchema: {
                              value: '@json(item().SinkObjectSettings).schema'
                              type: 'Expression'
                            }
                            sourceName: {
                              value: '@json(item().SourceObjectSettings).table'
                              type: 'Expression'
                            }
                            sourceSchema: {
                              value: '@json(item().SourceObjectSettings).schema'
                              type: 'Expression'
                            }
                            database: {
                              value: '@pipeline().parameters.databaseName'
                              type: 'Expression'
                            }
                            container: {
                              value: '@item().targetContainer'
                              type: 'Expression'
                            }
                            parameter: {
                              value: '@item().Parameter'
                              type: 'Expression'
                            }
                            folderName: {
                              value: '@item().folderName'
                              type: 'Expression'
                            }
                            tablePrefix: {
                              value: '@item().tablePrefix'
                              type: 'Expression'
                            }
                            storageAccount: {
                              value: '@json(item().SourceObjectSettings).storageAccount'
                              type: 'Expression'
                            }
                            PipelineStartTime: {
                              value: '@pipeline().parameters.PipelineStartTime'
                              type: 'Expression'
                            }
                          }
                        }
                      }
                      {
                        name: 'LogTransformFailure_FL_SP_EXTPQ'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'MDF_SCD1_EXTPQ'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'Snapshot'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: 'EXTPQ'
                              type: 'String'
                            }
                            errorText: {
                              value: {
                                value: '@activity(\'MDF_SCD1_EXTPQ\').error?.message'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            rowsCopied: {
                              value: null
                              type: 'Int64'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: 'view'
                              type: 'String'
                            }
                            status: {
                              value: 'failed'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                      {
                        name: 'Fail_LogTransformFail_FL_EXTPQ'
                        type: 'Fail'
                        dependsOn: [
                          {
                            activity: 'LogTransformFailure_FL_SP_EXTPQ'
                            dependencyConditions: [
                              'Completed'
                            ]
                          }
                        ]
                        userProperties: []
                        typeProperties: {
                          message: {
                            value: '@concat(\'Acitivity failed for object: \', item().SourceObjectSettings, \'. Please review the audit logs for more details.\')'
                            type: 'Expression'
                          }
                          errorCode: '3204'
                        }
                      }
                      {
                        name: 'LogTransformSuccess_FL_EXTPQ'
                        type: 'SqlServerStoredProcedure'
                        dependsOn: [
                          {
                            activity: 'MDF_SCD1_EXTPQ'
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
                          storedProcedureName: '[config].[sp_upd_AuditLog]'
                          storedProcedureParameters: {
                            activity: {
                              value: 'Snapshot'
                              type: 'String'
                            }
                            commenceDateTime: {
                              value: null
                              type: 'DateTime'
                            }
                            completeDateTime: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            elMethod: {
                              value: 'EXTPQ'
                              type: 'String'
                            }
                            errorText: {
                              value: null
                              type: 'String'
                            }
                            rowsCopied: {
                              value: {
                                value: '@activity(\'MDF_SCD1_EXTPQ\').output?.rowsCopied'
                                type: 'Expression'
                              }
                              type: 'Int64'
                            }
                            runDate: {
                              value: {
                                value: '@convertTimeZone(utcNow(),\'UTC\',\'Cen. Australia Standard Time\')'
                                type: 'Expression'
                              }
                              type: 'DateTime'
                            }
                            schedule: {
                              value: {
                                value: '@pipeline().parameters.TriggerName'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            schema: {
                              value: {
                                value: '@json(item().SourceObjectSettings).schema'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            sourceType: {
                              value: 'view'
                              type: 'String'
                            }
                            status: {
                              value: 'succeeded'
                              type: 'String'
                            }
                            table: {
                              value: {
                                value: '@json(item().SourceObjectSettings).table'
                                type: 'Expression'
                              }
                              type: 'String'
                            }
                            watermark: {
                              value: null
                              type: 'String'
                            }
                          }
                        }
                        linkedServiceName: {
                          referenceName: 'ls_azsqldb_metadatacontroldb'
                          type: 'LinkedServiceReference'
                        }
                      }
                    ]
                  }
                ]
                defaultActivities: [
                  {
                    name: 'Wait1'
                    type: 'Wait'
                    dependsOn: []
                    userProperties: []
                    typeProperties: {
                      waitTimeInSeconds: 1
                    }
                  }
                ]
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
      ObjectsPerGroupToCopy: {
        type: 'Array'
      }
      TriggerName: {
        type: 'string'
      }
      databaseName: {
        type: 'string'
      }
      PipelineStartTime: {
        type: 'string'
      }
    }
    folder: {
      name: 'SCD'
    }
    annotations: []
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_azsqldb_metadatacontroldb'
    '${factoryId}/pipelines/MDF_SCD1_VW'
    '${factoryId}/pipelines/MDF_SCD1_SP'
    '${factoryId}/pipelines/ADF_CETAS_SP'
    '${factoryId}/pipelines/MDF_SCD2_VW'
    '${factoryId}/pipelines/ADF_EXTPQ_TF'
  ]
}

resource factoryName_Transformation_MiddleLevel 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/Transformation_MiddleLevel'
  properties: {
    activities: [
      {
        name: 'DivideOneBatchIntoMultipleGroups'
        description: 'Divide objects from single batch into multiple sub parallel groups to avoid reaching the output limit of lookup activity.'
        type: 'ForEach'
        dependsOn: []
        userProperties: []
        typeProperties: {
          items: {
            value: '@range(0, add(div(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity),\n                    if(equals(mod(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity), 0), 0, 1)))'
            type: 'Expression'
          }
          isSequential: false
          batchCount: 50
          activities: [
            {
              name: 'GetObjectsPerGroupToCopy'
              description: 'Get objects (tables etc.) from control table required to be copied in this group. The order of objects to be copied following the TaskId in control table (ORDER BY [TaskId] DESC).'
              type: 'Lookup'
              dependsOn: []
              policy: {
                timeout: '0.01:00:00'
                retry: 0
                retryIntervalInSeconds: 30
                secureOutput: false
                secureInput: false
              }
              userProperties: []
              typeProperties: {
                source: {
                  type: 'AzureSqlSource'
                  sqlReaderQuery: {
                    value: 'WITH OrderedControlTable AS (\n                             SELECT *, ROW_NUMBER() OVER (ORDER BY [TaskId], [Id] DESC) AS RowNumber\n                             FROM @{pipeline().parameters.MainControlTableName}\n                             where TopLevelPipelineName = \'@{pipeline().parameters.TopLevelPipelineName}\'\n                             and TriggerName like \'%@{pipeline().parameters.TriggerName}%\' and stage = @{pipeline().parameters.stage} and CopyEnabled = 1)\n                             SELECT * FROM OrderedControlTable WHERE RowNumber BETWEEN @{add(mul(int(item()),pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity),\n                             add(mul(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.CurrentSequentialNumberOfBatch), 1))}\n                             AND @{min(add(mul(int(item()), pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity), add(mul(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, pipeline().parameters.CurrentSequentialNumberOfBatch),\n                             pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity)),\n                            mul(pipeline().parameters.SumOfObjectsToCopyForCurrentBatch, add(pipeline().parameters.CurrentSequentialNumberOfBatch,1)), pipeline().parameters.SumOfObjectsToCopy)}'
                    type: 'Expression'
                  }
                  queryTimeout: '02:00:00'
                  partitionOption: 'None'
                }
                dataset: {
                  referenceName: 'ds_azsqldb_sqldbcontroldb'
                  type: 'DatasetReference'
                  parameters: {}
                }
                firstRowOnly: false
              }
            }
            {
              name: 'CopyObjectsInOneGroup'
              description: 'Execute another pipeline to copy objects from one group. The objects belonging to this group will be copied parallelly.'
              type: 'ExecutePipeline'
              dependsOn: [
                {
                  activity: 'GetObjectsPerGroupToCopy'
                  dependencyConditions: [
                    'Succeeded'
                  ]
                }
              ]
              userProperties: []
              typeProperties: {
                pipeline: {
                  referenceName: 'Transformation_BottomLevel'
                  type: 'PipelineReference'
                }
                waitOnCompletion: true
                parameters: {
                  ObjectsPerGroupToCopy: {
                    value: '@activity(\'GetObjectsPerGroupToCopy\').output.value'
                    type: 'Expression'
                  }
                  TriggerName: {
                    value: '@pipeline().parameters.TriggerName'
                    type: 'Expression'
                  }
                  databaseName: {
                    value: '@pipeline().parameters.databaseName'
                    type: 'Expression'
                  }
                  PipelineStartTime: {
                    value: '@pipeline().parameters.PipelineStartTime'
                    type: 'Expression'
                  }
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
      MaxNumberOfObjectsReturnedFromLookupActivity: {
        type: 'Int'
      }
      TopLevelPipelineName: {
        type: 'String'
      }
      TriggerName: {
        type: 'String'
      }
      CurrentSequentialNumberOfBatch: {
        type: 'Int'
      }
      SumOfObjectsToCopy: {
        type: 'Int'
      }
      SumOfObjectsToCopyForCurrentBatch: {
        type: 'Int'
      }
      MainControlTableName: {
        type: 'String'
      }
      databaseName: {
        type: 'string'
      }
      stage: {
        type: 'int'
      }
      PipelineStartTime: {
        type: 'string'
      }
    }
    folder: {
      name: 'SCD'
    }
    annotations: []
  }
  dependsOn: [
    '${factoryId}/datasets/ds_azsqldb_sqldbcontroldb'
    '${factoryId}/pipelines/Transformation_BottomLevel'
  ]
}

resource factoryName_Transformation_StageLevel 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/Transformation_StageLevel'
  properties: {
    activities: [
      {
        name: 'GetStagesCount'
        type: 'Lookup'
        dependsOn: []
        policy: {
          timeout: '0.12:00:00'
          retry: 0
          retryIntervalInSeconds: 30
          secureOutput: false
          secureInput: false
        }
        userProperties: []
        typeProperties: {
          source: {
            type: 'AzureSqlSource'
            sqlReaderQuery: {
              value: 'SELECT distinct(stage) as stage FROM @{pipeline().parameters.MainControlTableName} where TriggerName like \'%@{pipeline().parameters.triggerName}%\' and CopyEnabled = 1 order by stage'
              type: 'Expression'
            }
            queryTimeout: '02:00:00'
            partitionOption: 'None'
          }
          dataset: {
            referenceName: 'ds_azsqldb_sqldbcontroldb'
            type: 'DatasetReference'
            parameters: {}
          }
          firstRowOnly: false
        }
      }
      {
        name: 'CopyBatchesPerStage'
        type: 'ForEach'
        dependsOn: [
          {
            activity: 'GetStagesCount'
            dependencyConditions: [
              'Succeeded'
            ]
          }
        ]
        userProperties: []
        typeProperties: {
          items: {
            value: '@activity(\'GetStagesCount\').output.value'
            type: 'Expression'
          }
          isSequential: true
          activities: [
            {
              name: 'Transformation_TopLevel'
              type: 'ExecutePipeline'
              dependsOn: []
              userProperties: []
              typeProperties: {
                pipeline: {
                  referenceName: 'Transformation_TopLevel'
                  type: 'PipelineReference'
                }
                waitOnCompletion: true
                parameters: {
                  databaseName: 'controldb'
                  MaxNumberOfObjectsReturnedFromLookupActivity: 5000
                  MaxNumberOfConcurrentTasks: 50
                  MainControlTableName: {
                    value: '@pipeline().parameters.MainControlTableName'
                    type: 'Expression'
                  }
                  stage: {
                    value: '@item().stage'
                    type: 'Expression'
                  }
                  trigger: {
                    value: '@pipeline().parameters.triggerName'
                    type: 'Expression'
                  }
                  PipelineStartTime: {
                    value: '@pipeline().parameters.PipelineStartTime'
                    type: 'Expression'
                  }
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
      MainControlTableName: {
        type: 'string'
        defaultValue: 'config.vw_MaterialisedTransform'
      }
      triggerName: {
        type: 'string'
        defaultValue: 'manual'
      }
      PipelineStartTime: {
        type: 'string'
      }
    }
    folder: {
      name: 'SCD'
    }
    annotations: []
  }
  dependsOn: [
    '${factoryId}/datasets/ds_azsqldb_sqldbcontroldb'
    '${factoryId}/pipelines/Transformation_TopLevel'
  ]
}

resource factoryName_Transformation_TopLevel 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/Transformation_TopLevel'
  properties: {
    activities: [
      {
        name: 'GetSumOfObjectsToCopy'
        description: 'Count the total number of objects (tables etc.) required to be copied in this run.'
        type: 'Lookup'
        dependsOn: []
        policy: {
          timeout: '0.00:05:00'
          retry: 1
          retryIntervalInSeconds: 30
          secureOutput: false
          secureInput: false
        }
        userProperties: []
        typeProperties: {
          source: {
            type: 'AzureSqlSource'
            sqlReaderQuery: {
              value: 'SELECT count(*) as count FROM @{pipeline().parameters.MainControlTableName} where TriggerName like \'%@{pipeline().parameters.trigger}%\' and stage = @{pipeline().parameters.stage} and CopyEnabled = 1'
              type: 'Expression'
            }
            queryTimeout: '02:00:00'
            partitionOption: 'None'
          }
          dataset: {
            referenceName: 'ds_azsqldb_satacsqldbcontroldb'
            type: 'DatasetReference'
            parameters: {}
          }
        }
      }
      {
        name: 'CopyBatchesOfObjectsSequentially'
        description: 'Come up with the number of sequential batches based on the max allowed concurrent copy tasks, and then execute another pipeline to copy different batches sequentially.'
        type: 'ForEach'
        dependsOn: [
          {
            activity: 'GetSumOfObjectsToCopy'
            dependencyConditions: [
              'Succeeded'
            ]
          }
        ]
        userProperties: []
        typeProperties: {
          items: {
            value: '@range(0, add(div(activity(\'GetSumOfObjectsToCopy\').output.firstRow.count,\n                    pipeline().parameters.MaxNumberOfConcurrentTasks),\n                    if(equals(mod(activity(\'GetSumOfObjectsToCopy\').output.firstRow.count,\n                    pipeline().parameters.MaxNumberOfConcurrentTasks), 0), 0, 1)))'
            type: 'Expression'
          }
          isSequential: true
          activities: [
            {
              name: 'CopyObjectsInOneBatch'
              description: 'Execute another pipeline to copy one batch of objects. The objects belonging to this batch will be copied parallelly.'
              type: 'ExecutePipeline'
              dependsOn: []
              userProperties: []
              typeProperties: {
                pipeline: {
                  referenceName: 'Transformation_MiddleLevel'
                  type: 'PipelineReference'
                }
                waitOnCompletion: true
                parameters: {
                  MaxNumberOfObjectsReturnedFromLookupActivity: {
                    value: '@pipeline().parameters.MaxNumberOfObjectsReturnedFromLookupActivity'
                    type: 'Expression'
                  }
                  TopLevelPipelineName: {
                    value: '@{pipeline().Pipeline}'
                    type: 'Expression'
                  }
                  TriggerName: {
                    value: '@pipeline().parameters.trigger'
                    type: 'Expression'
                  }
                  CurrentSequentialNumberOfBatch: {
                    value: '@item()'
                    type: 'Expression'
                  }
                  SumOfObjectsToCopy: {
                    value: '@activity(\'GetSumOfObjectsToCopy\').output.firstRow.count'
                    type: 'Expression'
                  }
                  SumOfObjectsToCopyForCurrentBatch: {
                    value: '@min(pipeline().parameters.MaxNumberOfConcurrentTasks, activity(\'GetSumOfObjectsToCopy\').output.firstRow.count)'
                    type: 'Expression'
                  }
                  MainControlTableName: {
                    value: '@pipeline().parameters.MainControlTableName'
                    type: 'Expression'
                  }
                  databaseName: {
                    value: '@pipeline().parameters.databaseName'
                    type: 'Expression'
                  }
                  stage: {
                    value: '@pipeline().parameters.stage'
                    type: 'Expression'
                  }
                  PipelineStartTime: {
                    value: '@pipeline().parameters.PipelineStartTime'
                    type: 'Expression'
                  }
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
      databaseName: {
        type: 'string'
        defaultValue: 'controldb'
      }
      MaxNumberOfObjectsReturnedFromLookupActivity: {
        type: 'int'
        defaultValue: 5000
      }
      MaxNumberOfConcurrentTasks: {
        type: 'int'
        defaultValue: 50
      }
      MainControlTableName: {
        type: 'string'
        defaultValue: 'config.vw_MaterialisedTransform'
      }
      stage: {
        type: 'int'
        defaultValue: 1
      }
      trigger: {
        type: 'string'
        defaultValue: 'manual'
      }
      PipelineStartTime: {
        type: 'string'
      }
    }
    folder: {
      name: 'SCD'
    }
    annotations: []
  }
  dependsOn: [
    '${factoryId}/datasets/ds_azsqldb_satacsqldbcontroldb'
    '${factoryId}/pipelines/Transformation_MiddleLevel'
  ]
}

resource factoryName_Transformation_Top_TopLevel 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${factoryName}/Transformation_Top_TopLevel'
  properties: {
    activities: [
      {
        name: 'GetTotalJobStagesPerTriggerName'
        type: 'Lookup'
        dependsOn: []
        policy: {
          timeout: '0.12:00:00'
          retry: 0
          retryIntervalInSeconds: 30
          secureOutput: false
          secureInput: false
        }
        userProperties: []
        typeProperties: {
          source: {
            type: 'AzureSqlSource'
            sqlReaderQuery: {
              value: 'select distinct(JobStage) as JobStage from @{pipeline().parameters.MainControlTableName} where JobTrigger like \'%@{pipeline().TriggerName}%\' order by jobStage'
              type: 'Expression'
            }
            queryTimeout: '02:00:00'
            partitionOption: 'None'
          }
          dataset: {
            referenceName: 'MetadataDrivenCopy_SQL_ControlDS'
            type: 'DatasetReference'
            parameters: {}
          }
          firstRowOnly: false
        }
      }
      {
        name: 'ForEachStagePerTrigger'
        type: 'ForEach'
        dependsOn: [
          {
            activity: 'GetTotalJobStagesPerTriggerName'
            dependencyConditions: [
              'Succeeded'
            ]
          }
        ]
        userProperties: []
        typeProperties: {
          items: {
            value: '@activity(\'GetTotalJobStagesPerTriggerName\').output.value'
            type: 'Expression'
          }
          isSequential: true
          activities: [
            {
              name: 'GetStageAttributes'
              type: 'Lookup'
              dependsOn: []
              policy: {
                timeout: '0.12:00:00'
                retry: 0
                retryIntervalInSeconds: 30
                secureOutput: false
                secureInput: false
              }
              userProperties: []
              typeProperties: {
                source: {
                  type: 'AzureSqlSource'
                  sqlReaderQuery: {
                    value: 'select PipelineName, ControlTableTrigger, JobStage from @{pipeline().parameters.MainControlTableName} where JobStage = @{item().JobStage} and JobTrigger like \'%@{pipeline().TriggerName}%\''
                    type: 'Expression'
                  }
                  queryTimeout: '02:00:00'
                  partitionOption: 'None'
                }
                dataset: {
                  referenceName: 'ds_azsqldb_sqldbcontroldb'
                  type: 'DatasetReference'
                  parameters: {}
                }
              }
            }
            {
              name: 'SwitchPipeline'
              type: 'Switch'
              dependsOn: [
                {
                  activity: 'GetStageAttributes'
                  dependencyConditions: [
                    'Succeeded'
                  ]
                }
              ]
              userProperties: []
              typeProperties: {
                on: {
                  value: '@activity(\'GetStageAttributes\').output.firstRow.PipelineName'
                  type: 'Expression'
                }
                cases: [
                  {
                    value: 'MetadataDrivenCopy_SQL_TopLevel'
                    activities: [
                      {
                        name: 'MetadataDrivenCopy_SQL_TopLevel'
                        type: 'ExecutePipeline'
                        dependsOn: []
                        userProperties: []
                        typeProperties: {
                          pipeline: {
                            referenceName: 'MetadataDrivenCopy_SQL_TopLevel'
                            type: 'PipelineReference'
                          }
                          waitOnCompletion: true
                          parameters: {
                            MaxNumberOfObjectsReturnedFromLookupActivity: 5000
                            MaxNumberOfConcurrentTasks: 16
                            MainControlTableName: 'config.SQLControlTable'
                            ConnectionControlTableName: 'config.SQLConnectionControlTable'
                            trigger: {
                              value: '@activity(\'GetStageAttributes\').output.firstRow.ControlTableTrigger'
                              type: 'Expression'
                            }
                            PipelineStartTime: {
                              value: '@pipeline().parameters.TriggerStartTime'
                              type: 'Expression'
                            }
                          }
                        }
                      }
                    ]
                  }
                  {
                    value: 'Transformation_StageLevel'
                    activities: [
                      {
                        name: 'Transformation_StageLevel'
                        type: 'ExecutePipeline'
                        dependsOn: []
                        userProperties: []
                        typeProperties: {
                          pipeline: {
                            referenceName: 'Transformation_StageLevel'
                            type: 'PipelineReference'
                          }
                          waitOnCompletion: true
                          parameters: {
                            MainControlTableName: 'config.vw_MaterialisedTransform'
                            triggerName: {
                              value: '@activity(\'GetStageAttributes\').output.firstRow.ControlTableTrigger'
                              type: 'Expression'
                            }
                            PipelineStartTime: {
                              value: '@pipeline().parameters.TriggerStartTime'
                              type: 'Expression'
                            }
                          }
                        }
                      }
                    ]
                  }
                ]
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
      MainControlTableName: {
        type: 'string'
        defaultValue: 'config.JobControl'
      }
      TriggerStartTime: {
        type: 'string'
      }
    }
    variables: {
      Stage_String: {
        type: 'String'
      }
    }
    annotations: []
  }
  dependsOn: [
    '${factoryId}/datasets/MetadataDrivenCopy_SQL_ControlDS'
    '${factoryId}/datasets/ds_azsqldb_sqldbcontroldb'
    '${factoryId}/pipelines/MetadataDrivenCopy_SQL_TopLevel'
    '${factoryId}/pipelines/Transformation_StageLevel'
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

resource factoryName_MetadataDrivenCopy_FileSystem_ParquetSourceDS 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/MetadataDrivenCopy_FileSystem_ParquetSourceDS'
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
      cw_container: {
        type: 'string'
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
          value: '@concat(dataset().cw_fileSystem,\'/\',dataset().cw_folderPath,\'/\',formatDateTime(convertTimeZone(pipeline().TriggerTime,\'UTC\',\'Cen. Australia Standard Time\'), \'yyyy/MM/dd\'))'
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

resource factoryName_ds_azsqldb_satacsqldbcontroldb 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${factoryName}/ds_azsqldb_satacsqldbcontroldb'
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

resource factoryName_MDF_SCD1_Initial_SP 'Microsoft.DataFactory/factories/dataflows@2018-06-01' = {
  name: '${factoryName}/MDF_SCD1_Initial_SP'
  properties: {
    description: 'Executes an initial load to the target environment, based on the parsed parameters.'
    folder: {
      name: 'SCD/1/Initial'
    }
    type: 'MappingDataFlow'
    typeProperties: {
      sources: [
        {
          linkedService: {
            referenceName: 'ls_synapsesqlondemand_gen01'
            type: 'LinkedServiceReference'
          }
          name: 'synSslSrcQry'
        }
      ]
      sinks: [
        {
          linkedService: {
            referenceName: 'ls_azdatalake'
            type: 'LinkedServiceReference'
          }
          name: 'sinkDelta'
        }
      ]
      transformations: [
        {
          name: 'derivedColumns'
        }
        {
          name: 'surrogateKey'
        }
        {
          name: 'alterRows'
        }
      ]
      scriptLines: [
        'parameters{'
        '     sourceName as string (\'vw_empty_hash\'),'
        '     sourceSchema as string (\'dbo\'),'
        '     targetName as string (\'Org_Hierarchy\'),'
        '     targetSchema as string (\'fnd_rel\')'
        '}'
        'source(output('
        '          rowSig as binary,'
        '          rowHash as binary,'
        '          SK as integer'
        '     ),'
        '     allowSchemaDrift: true,'
        '     validateSchema: false,'
        '     format: \'table\','
        '     store: \'synapseanalytics\','
        '     schemaName: ($sourceSchema),'
        '     tableName: ($sourceName),'
        '     isolationLevel: \'READ_UNCOMMITTED\','
        '     staged: false) ~> synSslSrcQry'
        'synSslSrcQry derive(rowUUID = uuid(),'
        '          lastUpdateDate = currentTimestamp()) ~> derivedColumns'
        'derivedColumns keyGenerate(output(SK as long),'
        '     startAt: 1L,'
        '     stepValue: 1L) ~> surrogateKey'
        'surrogateKey alterRow(upsertIf(true())) ~> alterRows'
        'alterRows sink(allowSchemaDrift: true,'
        '     validateSchema: false,'
        '     format: \'delta\','
        '     compressionType: \'snappy\','
        '     compressionLevel: \'Fastest\','
        '     fileSystem: \'transformed\','
        '     folderPath: (concat(\'Delta/\',$targetSchema,\'/\',$targetName)),'
        '     mergeSchema: false,'
        '     autoCompact: false,'
        '     optimizedWrite: false,'
        '     vacuum: 0,'
        '     deletable: false,'
        '     insertable: true,'
        '     updateable: false,'
        '     upsertable: false,'
        '     skipDuplicateMapInputs: true,'
        '     skipDuplicateMapOutputs: true,'
        '     preCommands: [],'
        '     postCommands: []) ~> sinkDelta'
      ]
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_synapsesqlondemand_gen01'
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}

resource factoryName_MDF_SCD1_Initial_VW 'Microsoft.DataFactory/factories/dataflows@2018-06-01' = {
  name: '${factoryName}/MDF_SCD1_Initial_VW'
  properties: {
    description: 'Executes an initial load to the target environment, based on the parsed parameters.'
    folder: {
      name: 'SCD/1/Initial'
    }
    type: 'MappingDataFlow'
    typeProperties: {
      sources: [
        {
          linkedService: {
            referenceName: 'ls_synapsesqlondemand_gen01'
            type: 'LinkedServiceReference'
          }
          name: 'synSslSrcQry'
        }
      ]
      sinks: [
        {
          linkedService: {
            referenceName: 'ls_azdatalake'
            type: 'LinkedServiceReference'
          }
          name: 'sinkDelta'
        }
      ]
      transformations: [
        {
          name: 'derivedColumns'
        }
        {
          name: 'surrogateKey'
        }
        {
          name: 'alterRows'
        }
      ]
      scriptLines: [
        'parameters{'
        '     sourceName as string (\'vw_empty_hash\'),'
        '     sourceSchema as string (\'dbo\'),'
        '     targetName as string (\'empty\'),'
        '     targetSchema as string (\'fnd_rel\')'
        '}'
        'source(output('
        '          rowSig as binary,'
        '          rowHash as binary'
        '     ),'
        '     allowSchemaDrift: true,'
        '     validateSchema: false,'
        '     format: \'table\','
        '     store: \'synapseanalytics\','
        '     schemaName: ($sourceSchema),'
        '     tableName: ($sourceName),'
        '     isolationLevel: \'READ_UNCOMMITTED\','
        '     staged: false) ~> synSslSrcQry'
        'synSslSrcQry derive(rowUUID = uuid(),'
        '          lastUpdateDate = currentTimestamp()) ~> derivedColumns'
        'derivedColumns keyGenerate(output(SK as long),'
        '     startAt: 1L,'
        '     stepValue: 1L) ~> surrogateKey'
        'surrogateKey alterRow(upsertIf(true())) ~> alterRows'
        'alterRows sink(allowSchemaDrift: true,'
        '     validateSchema: false,'
        '     format: \'delta\','
        '     compressionType: \'snappy\','
        '     compressionLevel: \'Fastest\','
        '     fileSystem: \'transformed\','
        '     folderPath: (concat(\'Delta/\',$targetSchema,\'/\',$targetName)),'
        '     mergeSchema: false,'
        '     autoCompact: false,'
        '     optimizedWrite: false,'
        '     vacuum: 0,'
        '     deletable: false,'
        '     insertable: true,'
        '     updateable: false,'
        '     upsertable: false,'
        '     skipDuplicateMapInputs: true,'
        '     skipDuplicateMapOutputs: true,'
        '     preCommands: [],'
        '     postCommands: []) ~> sinkDelta'
      ]
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_synapsesqlondemand_gen01'
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}

resource factoryName_MDF_SCD1_Update_SP 'Microsoft.DataFactory/factories/dataflows@2018-06-01' = {
  name: '${factoryName}/MDF_SCD1_Update_SP'
  properties: {
    description: 'Execute a merge (update), to the target directory, based on the provided parameters.'
    folder: {
      name: 'SCD/1/Update'
    }
    type: 'MappingDataFlow'
    typeProperties: {
      sources: [
        {
          linkedService: {
            referenceName: 'ls_synapsesqlondemand_gen01'
            type: 'LinkedServiceReference'
          }
          name: 'synSslSrcQry'
        }
        {
          linkedService: {
            referenceName: 'ls_synapsesqlondemand_gen01'
            type: 'LinkedServiceReference'
          }
          name: 'synSqlCur'
        }
      ]
      sinks: [
        {
          linkedService: {
            referenceName: 'ls_azdatalake'
            type: 'LinkedServiceReference'
          }
          name: 'sinkDelta'
        }
      ]
      transformations: [
        {
          name: 'derivedColumns'
        }
        {
          name: 'surrogateKey'
        }
        {
          name: 'CheckBKExists'
        }
        {
          name: 'getMaxSK'
        }
        {
          name: 'SeedNewSKs'
        }
        {
          name: 'UpdateSK'
        }
        {
          name: 'DropUnwanted'
        }
        {
          name: 'derivedColumnCur'
        }
        {
          name: 'TagInsert'
        }
        {
          name: 'TagUpdate'
        }
        {
          name: 'CheckBKUpdates'
        }
        {
          name: 'UnionInsertupdate'
        }
        {
          name: 'lookupSK'
        }
        {
          name: 'Keys'
        }
        {
          name: 'RemoveExtraRowsig'
        }
      ]
      scriptLines: [
        'parameters{'
        '     sourceName as string (\'vw_empty_hash\'),'
        '     sourceSchema as string (\'dbo\'),'
        '     targetName as string (\'usp_Org_Hierarchy\'),'
        '     targetSchema as string (\'fnd_rel\')'
        '}'
        'source(output('
        '          rowSig as binary,'
        '          rowHash as binary,'
        '          SK as integer'
        '     ),'
        '     allowSchemaDrift: true,'
        '     validateSchema: false,'
        '     format: \'table\','
        '     store: \'synapseanalytics\','
        '     schemaName: ($sourceSchema),'
        '     tableName: ($sourceName),'
        '     isolationLevel: \'READ_UNCOMMITTED\','
        '     staged: false) ~> synSslSrcQry'
        'source(output('
        '          rowSig as binary,'
        '          rowHash as binary,'
        '          SK as integer'
        '     ),'
        '     allowSchemaDrift: true,'
        '     validateSchema: false,'
        '     format: \'table\','
        '     store: \'synapseanalytics\','
        '     schemaName: ($sourceSchema),'
        '     tableName: ($sourceName),'
        '     isolationLevel: \'READ_UNCOMMITTED\','
        '     staged: false) ~> synSqlCur'
        'synSslSrcQry derive(rowUUID = uuid(),'
        '          lastUpdateDate = currentTimestamp()) ~> derivedColumns'
        'CheckBKExists keyGenerate(output(SK as long),'
        '     startAt: 1L,'
        '     stepValue: 1L) ~> surrogateKey'
        'derivedColumns, synSqlCur exists(equals(synSslSrcQry@rowSig, synSqlCur@rowSig),'
        '     negate:true,'
        '     broadcast: \'auto\')~> CheckBKExists'
        'synSqlCur aggregate(MaxSK = max(toInteger(byName(\'SK\')))) ~> getMaxSK'
        'surrogateKey, getMaxSK join(MaxSK==SK||true(),'
        '     joinType:\'cross\','
        '     matchType:\'exact\','
        '     ignoreSpaces: false,'
        '     broadcast: \'auto\')~> SeedNewSKs'
        'SeedNewSKs derive(SK = MaxSK+SK) ~> UpdateSK'
        'UpdateSK select(mapColumn('
        '          each(match(!in([\'MaxSK\'],name)))'
        '     ),'
        '     skipDuplicateMapInputs: true,'
        '     skipDuplicateMapOutputs: true) ~> DropUnwanted'
        'synSqlCur derive(rowUUID = uuid(),'
        '          lastUpdateDate = currentTimestamp()) ~> derivedColumnCur'
        'DropUnwanted alterRow(insertIf(true())) ~> TagInsert'
        'RemoveExtraRowsig alterRow(updateIf(true())) ~> TagUpdate'
        'derivedColumns, synSqlCur exists(equals(synSslSrcQry@rowSig, synSqlCur@rowSig) && not(equals(synSslSrcQry@rowHash, synSqlCur@rowHash)),'
        '     negate:false,'
        '     broadcast: \'auto\')~> CheckBKUpdates'
        'TagInsert, TagUpdate union(byName: true)~> UnionInsertupdate'
        'CheckBKUpdates, Keys lookup(toString(synSslSrcQry@rowSig) == toString(Keys@rowSig),'
        '     multiple: false,'
        '     pickup: \'any\','
        '     broadcast: \'auto\')~> lookupSK'
        'derivedColumnCur select(mapColumn('
        '          rowSig,'
        '          SK'
        '     ),'
        '     skipDuplicateMapInputs: true,'
        '     skipDuplicateMapOutputs: true) ~> Keys'
        'lookupSK select(mapColumn('
        '          each(match(!in([\'keys@rowSig\'],name)))'
        '     ),'
        '     skipDuplicateMapInputs: true,'
        '     skipDuplicateMapOutputs: true) ~> RemoveExtraRowsig'
        'UnionInsertupdate sink(allowSchemaDrift: true,'
        '     validateSchema: false,'
        '     input('
        '          ATSIID as string,'
        '          ATSI as string,'
        '          ATSIGroup as string,'
        '          ATSIGroupUnknown as string,'
        '          AlternativeATSI as string,'
        '          IndigenousID as string,'
        '          ValeoATSIID as string,'
        '          EYSATSIID as string,'
        '          NAPLANATSIID as integer,'
        '          PATATSIID as string,'
        '          BPATHATSIID as string,'
        '          Ordinal as integer,'
        '          Boolean as boolean,'
        '          rowSig as binary,'
        '          rowHash as binary,'
        '          rowUUID as string,'
        '          lastUpdateDate as timestamp,'
        '          SK as long'
        '     ),'
        '     format: \'delta\','
        '     compressionType: \'snappy\','
        '     compressionLevel: \'Fastest\','
        '     fileSystem: \'transformed\','
        '     folderPath: (concat(\'Delta/\',$targetSchema,\'/\',$targetName)),'
        '     mergeSchema: false,'
        '     autoCompact: true,'
        '     optimizedWrite: true,'
        '     vacuum: 0,'
        '     deletable: false,'
        '     insertable: true,'
        '     updateable: true,'
        '     upsertable: false,'
        '     keys:[\'SK\'],'
        '     skipDuplicateMapInputs: true,'
        '     skipDuplicateMapOutputs: true,'
        '     preCommands: [],'
        '     postCommands: []) ~> sinkDelta'
      ]
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_synapsesqlondemand_gen01'
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}

resource factoryName_MDF_SCD1_Update_VW 'Microsoft.DataFactory/factories/dataflows@2018-06-01' = {
  name: '${factoryName}/MDF_SCD1_Update_VW'
  properties: {
    description: 'Execute a merge (update), to the target directory, based on the provided parameters.'
    folder: {
      name: 'SCD/1/Update'
    }
    type: 'MappingDataFlow'
    typeProperties: {
      sources: [
        {
          linkedService: {
            referenceName: 'ls_synapsesqlondemand_gen01'
            type: 'LinkedServiceReference'
          }
          name: 'synSslSrcQry'
        }
        {
          linkedService: {
            referenceName: 'ls_synapsesqlondemand_gen01'
            type: 'LinkedServiceReference'
          }
          name: 'synSqlCur'
        }
      ]
      sinks: [
        {
          linkedService: {
            referenceName: 'ls_azdatalake'
            type: 'LinkedServiceReference'
          }
          name: 'sinkDelta'
        }
      ]
      transformations: [
        {
          name: 'derivedColumns'
        }
        {
          name: 'surrogateKey'
        }
        {
          name: 'CheckBKExists'
        }
        {
          name: 'getMaxSK'
        }
        {
          name: 'SeedNewSKs'
        }
        {
          name: 'UpdateSK'
        }
        {
          name: 'DropUnwanted'
        }
        {
          name: 'derivedColumnCur'
        }
        {
          name: 'TagInsert'
        }
        {
          name: 'TagUpdate'
        }
        {
          name: 'CheckBKUpdates'
        }
        {
          name: 'UnionInsertupdate'
        }
        {
          name: 'lookupSK'
        }
        {
          name: 'Keys'
        }
        {
          name: 'RemoveExtraRowsig'
        }
      ]
      scriptLines: [
        'parameters{'
        '     sourceName as string (\'vw_empty_hash\'),'
        '     sourceSchema as string (\'dbo\'),'
        '     targetName as string (\'empty\'),'
        '     targetSchema as string (\'fnd_rel\')'
        '}'
        'source(output('
        '          rowSig as binary,'
        '          rowHash as binary'
        '     ),'
        '     allowSchemaDrift: true,'
        '     validateSchema: false,'
        '     format: \'table\','
        '     store: \'synapseanalytics\','
        '     schemaName: ($sourceSchema),'
        '     tableName: ($sourceName),'
        '     isolationLevel: \'READ_UNCOMMITTED\','
        '     staged: false) ~> synSslSrcQry'
        'source(output('
        '          rowSig as binary,'
        '          rowHash as binary,'
        '          SK as integer'
        '     ),'
        '     allowSchemaDrift: true,'
        '     validateSchema: false,'
        '     format: \'table\','
        '     store: \'synapseanalytics\','
        '     schemaName: ($sourceSchema),'
        '     tableName: ($sourceName),'
        '     isolationLevel: \'READ_UNCOMMITTED\','
        '     staged: false) ~> synSqlCur'
        'synSslSrcQry derive(rowUUID = uuid(),'
        '          lastUpdateDate = currentTimestamp()) ~> derivedColumns'
        'CheckBKExists keyGenerate(output(SK as long),'
        '     startAt: 1L,'
        '     stepValue: 1L) ~> surrogateKey'
        'derivedColumns, synSqlCur exists(equals(synSslSrcQry@rowSig, synSqlCur@rowSig),'
        '     negate:true,'
        '     broadcast: \'auto\')~> CheckBKExists'
        'synSqlCur aggregate(maxSK = max(toInteger(byName(\'SK\')))) ~> getMaxSK'
        'surrogateKey, getMaxSK join(maxSK==SK||true(),'
        '     joinType:\'cross\','
        '     matchType:\'exact\','
        '     ignoreSpaces: false,'
        '     broadcast: \'auto\')~> SeedNewSKs'
        'SeedNewSKs derive(SK = maxSK+SK) ~> UpdateSK'
        'UpdateSK select(mapColumn('
        '          each(match(!in([\'MaxSK\'],name)))'
        '     ),'
        '     skipDuplicateMapInputs: true,'
        '     skipDuplicateMapOutputs: true) ~> DropUnwanted'
        'synSqlCur derive(rowUUID = uuid(),'
        '          lastUpdateDate = currentTimestamp()) ~> derivedColumnCur'
        'DropUnwanted alterRow(insertIf(true())) ~> TagInsert'
        'RemoveExtraRowsig alterRow(updateIf(true())) ~> TagUpdate'
        'derivedColumns, synSqlCur exists(equals(synSslSrcQry@rowSig, synSqlCur@rowSig) && not(equals(synSslSrcQry@rowHash, synSqlCur@rowHash)),'
        '     negate:false,'
        '     broadcast: \'auto\')~> CheckBKUpdates'
        'TagInsert, TagUpdate union(byName: true)~> UnionInsertupdate'
        'CheckBKUpdates, Keys lookup(toString(synSslSrcQry@rowSig) == toString(Keys@rowSig),'
        '     multiple: false,'
        '     pickup: \'any\','
        '     broadcast: \'auto\')~> lookupSK'
        'derivedColumnCur select(mapColumn('
        '          rowSig,'
        '          SK'
        '     ),'
        '     skipDuplicateMapInputs: true,'
        '     skipDuplicateMapOutputs: true) ~> Keys'
        'lookupSK select(mapColumn('
        '          each(match(!in([\'keys@rowSig\'],name)))'
        '     ),'
        '     skipDuplicateMapInputs: true,'
        '     skipDuplicateMapOutputs: true) ~> RemoveExtraRowsig'
        'UnionInsertupdate sink(allowSchemaDrift: true,'
        '     validateSchema: false,'
        '     input('
        '          ATSIID as string,'
        '          ATSI as string,'
        '          ATSIGroup as string,'
        '          ATSIGroupUnknown as string,'
        '          AlternativeATSI as string,'
        '          IndigenousID as string,'
        '          ValeoATSIID as string,'
        '          EYSATSIID as string,'
        '          NAPLANATSIID as integer,'
        '          PATATSIID as string,'
        '          BPATHATSIID as string,'
        '          Ordinal as integer,'
        '          Boolean as boolean,'
        '          rowSig as binary,'
        '          rowHash as binary,'
        '          rowUUID as string,'
        '          lastUpdateDate as timestamp,'
        '          SK as long'
        '     ),'
        '     format: \'delta\','
        '     compressionType: \'snappy\','
        '     compressionLevel: \'Fastest\','
        '     fileSystem: \'transformed\','
        '     folderPath: (concat(\'Delta/\',$targetSchema,\'/\',$targetName)),'
        '     mergeSchema: false,'
        '     autoCompact: true,'
        '     optimizedWrite: true,'
        '     vacuum: 0,'
        '     deletable: false,'
        '     insertable: true,'
        '     updateable: true,'
        '     upsertable: false,'
        '     keys:[\'SK\'],'
        '     skipDuplicateMapInputs: true,'
        '     skipDuplicateMapOutputs: true,'
        '     preCommands: [],'
        '     postCommands: []) ~> sinkDelta'
      ]
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_synapsesqlondemand_gen01'
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}

resource factoryName_MDF_SCD2_Initial_VW 'Microsoft.DataFactory/factories/dataflows@2018-06-01' = {
  name: '${factoryName}/MDF_SCD2_Initial_VW'
  properties: {
    description: 'Executes an initial load to the target environment, based on the parsed parameters.'
    folder: {
      name: 'SCD/2/Initial'
    }
    type: 'MappingDataFlow'
    typeProperties: {
      sources: [
        {
          linkedService: {
            referenceName: 'ls_synapsesqlondemand_gen01'
            type: 'LinkedServiceReference'
          }
          name: 'synSslSrcQry'
        }
      ]
      sinks: [
        {
          linkedService: {
            referenceName: 'ls_azdatalake'
            type: 'LinkedServiceReference'
          }
          name: 'sinkDelta'
        }
      ]
      transformations: [
        {
          name: 'derivedColumns'
        }
        {
          name: 'surrogateKey'
        }
        {
          name: 'alterRows'
        }
      ]
      scriptLines: [
        'parameters{'
        '     sourceName as string (\'vw_Heims\'),'
        '     sourceSchema as string (\'trn_base_extract\'),'
        '     targetName as string (\'heims1\'),'
        '     targetSchema as string (\'trn_bus\')'
        '}'
        'source(allowSchemaDrift: true,'
        '     validateSchema: false,'
        '     format: \'table\','
        '     store: \'synapseanalytics\','
        '     schemaName: ($sourceSchema),'
        '     tableName: ($sourceName),'
        '     isolationLevel: \'READ_UNCOMMITTED\','
        '     staged: false) ~> synSslSrcQry'
        'synSslSrcQry derive(rowUUID = uuid(),'
        '          isCurrent = 1,'
        '          effectiveFromDT = currentTimestamp(),'
        '          effectiveToDT = toTimestamp(null())) ~> derivedColumns'
        'derivedColumns keyGenerate(output(SK as long),'
        '     startAt: 1L,'
        '     stepValue: 1L) ~> surrogateKey'
        'surrogateKey alterRow(upsertIf(true())) ~> alterRows'
        'alterRows sink(allowSchemaDrift: true,'
        '     validateSchema: false,'
        '     format: \'delta\','
        '     compressionType: \'snappy\','
        '     compressionLevel: \'Fastest\','
        '     fileSystem: \'transformed\','
        '     folderPath: (concat(\'Delta/\',$targetSchema,\'/\',$targetName)),'
        '     mergeSchema: false,'
        '     autoCompact: false,'
        '     optimizedWrite: false,'
        '     vacuum: 0,'
        '     deletable: false,'
        '     insertable: true,'
        '     updateable: false,'
        '     upsertable: false,'
        '     skipDuplicateMapInputs: true,'
        '     skipDuplicateMapOutputs: true,'
        '     preCommands: [],'
        '     postCommands: []) ~> sinkDelta'
      ]
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_synapsesqlondemand_gen01'
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}

resource factoryName_MDF_SCD2_Update_VW 'Microsoft.DataFactory/factories/dataflows@2018-06-01' = {
  name: '${factoryName}/MDF_SCD2_Update_VW'
  properties: {
    description: 'Execute a merge (update) scd2, to the target directory, based on the provided parameters.'
    folder: {
      name: 'SCD/2/Update'
    }
    type: 'MappingDataFlow'
    typeProperties: {
      sources: [
        {
          linkedService: {
            referenceName: 'ls_synapsesqlondemand_gen01'
            type: 'LinkedServiceReference'
          }
          name: 'SynSqlCur'
        }
        {
          linkedService: {
            referenceName: 'ls_synapsesqlondemand_gen01'
            type: 'LinkedServiceReference'
          }
          name: 'SynSslSrcQuery'
        }
      ]
      sinks: [
        {
          linkedService: {
            referenceName: 'ls_azdatalake'
            type: 'LinkedServiceReference'
          }
          name: 'SinkDelta'
        }
      ]
      transformations: [
        {
          name: 'TagInsert'
        }
        {
          name: 'surrogateKey1'
        }
        {
          name: 'AddHashInput'
        }
        {
          name: 'FilterActive'
        }
        {
          name: 'NewAndUpdate'
        }
        {
          name: 'GetMaxSK'
        }
        {
          name: 'JoinWithMax'
        }
        {
          name: 'UpdateSK'
        }
        {
          name: 'FilterForUpdated'
        }
        {
          name: 'UpdateValues'
        }
        {
          name: 'TagUpdate'
        }
        {
          name: 'UnionInsertUpdate'
        }
        {
          name: 'MapDriftedHash'
          description: 'Creates an explicit mapping for each drifted column'
        }
        {
          name: 'DropUnwanted'
        }
      ]
      scriptLines: [
        'parameters{'
        '     sourceName as string (\'vw_empty_hash_scd2\'),'
        '     sourceSchema as string (\'dbo\'),'
        '     targetName as string (\'empty_scd2\'),'
        '     targetSchema as string (\'fnd_rel\')'
        '}'
        'source(allowSchemaDrift: true,'
        '     validateSchema: false,'
        '     format: \'table\','
        '     store: \'synapseanalytics\','
        '     schemaName: ($sourceSchema),'
        '     tableName: ($sourceName),'
        '     isolationLevel: \'READ_UNCOMMITTED\','
        '     staged: false) ~> SynSqlCur'
        'source(allowSchemaDrift: true,'
        '     validateSchema: false,'
        '     format: \'table\','
        '     store: \'synapseanalytics\','
        '     schemaName: ($sourceSchema),'
        '     tableName: ($sourceName),'
        '     isolationLevel: \'READ_UNCOMMITTED\','
        '     staged: false) ~> SynSslSrcQuery'
        'DropUnwanted alterRow(insertIf(true())) ~> TagInsert'
        'NewAndUpdate keyGenerate(output(SK as long),'
        '     startAt: 1L,'
        '     stepValue: 1L) ~> surrogateKey1'
        'SynSslSrcQuery derive(rowUUID = uuid(),'
        '          rowSig = toBinary(byName(\'rowSig\')),'
        '          rowHash = toBinary(byName(\'rowHash\')),'
        '          isCurrent = 1,'
        '          effectiveFromDT = currentTimestamp(),'
        '          effectiveToDT = toTimestamp(null())) ~> AddHashInput'
        'MapDriftedHash filter(isCurrent == 1) ~> FilterActive'
        'AddHashInput, FilterActive exists(AddHashInput@rowSig == toBinary(byName(\'rowSig\',\'MapDriftedHash\'))\r'
        '     && AddHashInput@rowHash == toBinary(byName(\'rowHash\',\'MapDriftedHash\')),'
        '     negate:true,'
        '     broadcast: \'auto\')~> NewAndUpdate'
        'FilterActive aggregate(MaxSK = max(toInteger(byName(\'SK\')))) ~> GetMaxSK'
        'surrogateKey1, GetMaxSK join(SK==MaxSK || true(),'
        '     joinType:\'cross\','
        '     matchType:\'exact\','
        '     ignoreSpaces: false,'
        '     broadcast: \'auto\')~> JoinWithMax'
        'JoinWithMax derive(SK = SK + MaxSK) ~> UpdateSK'
        'FilterActive, NewAndUpdate exists(MapDriftedHash@rowSig == AddHashInput@rowSig,'
        '     negate:false,'
        '     broadcast: \'auto\')~> FilterForUpdated'
        'FilterForUpdated derive(isCurrent = 0,'
        '          effectiveToDT = currentTimestamp()) ~> UpdateValues'
        'UpdateValues alterRow(updateIf(true())) ~> TagUpdate'
        'TagInsert, TagUpdate union(byName: true)~> UnionInsertUpdate'
        'SynSqlCur derive(rowUUID = toString(byName(\'rowUUID\')),'
        '          rowSig = toBinary(byName(\'rowSig\')),'
        '          rowHash = toBinary(byName(\'rowHash\')),'
        '          isCurrent = toInteger(byName(\'isCurrent\')),'
        '          effectiveFromDT = toTimestamp(byName(\'effectiveFromDT\')),'
        '          effectiveToDT = toTimestamp(byName(\'effectiveToDT\')),'
        '          SK = toLong(byName(\'SK\'))) ~> MapDriftedHash'
        'UpdateSK select(mapColumn('
        '          each(match(!in([\'MaxSK\'],name)))'
        '     ),'
        '     skipDuplicateMapInputs: true,'
        '     skipDuplicateMapOutputs: true) ~> DropUnwanted'
        'UnionInsertUpdate sink(allowSchemaDrift: true,'
        '     validateSchema: false,'
        '     format: \'delta\','
        '     compressionType: \'snappy\','
        '     compressionLevel: \'Fastest\','
        '     fileSystem: \'transformed\','
        '     folderPath: (concat(\'Delta/\',$targetSchema,\'/\',$targetName)),'
        '     mergeSchema: false,'
        '     autoCompact: false,'
        '     optimizedWrite: false,'
        '     vacuum: 0,'
        '     deletable: false,'
        '     insertable: true,'
        '     updateable: true,'
        '     upsertable: false,'
        '     keys:[\'SK\'],'
        '     skipDuplicateMapInputs: true,'
        '     skipDuplicateMapOutputs: true,'
        '     preCommands: [],'
        '     postCommands: []) ~> SinkDelta'
      ]
    }
  }
  dependsOn: [
    '${factoryId}/linkedServices/ls_synapsesqlondemand_gen01'
    '${factoryId}/linkedServices/ls_azdatalake'
  ]
}