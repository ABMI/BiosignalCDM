library(dplyr)
library(EcgMeasurement)
# USER INPUTS
#=======================
# The folder where the study intermediate and result files will be written:
outputFolder <- "./EcgMeasurementResults"

# Specify where the temporary files (used by the ff package) will be created:
options(fftempdir ="/home/cbj/temp")

# Details for connecting to the server:
dbms <- "sql server"
user <- 'cbj'
pw <- 'qwer1234!@'
server <- '128.1.99.58'
port <- 1433

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                server = server,
                                                                user = user,
                                                                password = pw,
                                                                port = port)

# Add the database containing the OMOP CDM data
cdmDatabaseSchema <- 'cdm db'
# Add a sharebale name for the database containing the OMOP CDM data
cdmDatabaseName <- 'dbname'
# Add a database with read/write access as this is where the cohorts will be generated
cohortDatabaseSchema <- 'cohortDB.dbo'
databaseId <- "dbid"
databaseName <- "dbname"
databaseDescription <- ""
oracleTempSchema <- NULL

# table name where the cohorts will be generated
cohortTable <- 'cohorttablename'
#=======================

execute(connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cdmDatabaseName = cdmDatabaseName,
        cohortDatabaseSchema = cohortDatabaseSchema,
		oracleTempSchema = oracleTempSchema,
        cohortTable = cohortTable,
        outputFolder = outputFolder,
        createProtocol = F,
        createCohorts = F,
        runAnalyses = F, 
        createResultsDoc = F,
        packageResults = F,
        createValidationPackage = F,  
        #analysesToValidate = 1,
        minCellCount= 5,
        createShiny = F,
        createJournalDocument = F,
        analysisIdDocument = 1)



# Return ECGsPath with Binary outcome 
get_TargetEcgsWithOutcome <- function(PopulationPath=NULL,
                                      OutputPath='./TargetEcgsWithOutcome.csv',
                                      OutcomeAsValue=FALSE, 
                                      OutcomeAsValueDomain='Measurement', 
                                      OutcomeAsValueConceptId=c(3004410,42529253),
                                      OutcomeAsValueStartDays=0,
                                      OutcomeAsValueEndDays=90, 
                                      OutcomeAsValueAggregation='Mean',
                                      OutcomeAsValueAsRange=FALSE,
                                      EcgStartDaysBeforeIndex=-30, 
                                      EcgEndDaysBeforeIndex=0, 
                                      EcgAggregation='All'){ #function start
  if (is.null(PopulationPath)) {
    writeLines("Specify PopulationPath!")
  }
  library(dplyr)
  writeLines(sprintf("Selected Popultation : %s",PopulationPath))
  PopulationDf <- readRDS(file.path(outputFolder, PopulationPath))
  writeLines(sprintf('Number of entrys in target cohort : %s.',nrow(PopulationDf)))
  writeLines(sprintf('Number of patients which have proper ECG data : %s.',length(unique(PopulationDf$subjectId))))
  conn<-DatabaseConnector::connect(connectionDetails)
  
  writeLines("Connected to CDM Database")
  
  # OutcomeCount : Binary outcome to Lab value data
  writeLines(sprintf('Using value(measurement or observation) as outcome : %s',OutcomeAsValue ))
  if (!identical(OutcomeAsValue,FALSE)) {
    writeLines("Extracting values... : Start")
    VectorOfConcepts <- paste(OutcomeAsValueConceptId, collapse = ',')
    #VectorOfConceptsWithoutC <- substr(VectorOfConcepts, start=2, stop=nchar(VectorOfConcepts))
    if (!OutcomeAsValueDomain %in% c('Measurement', 'Observation')){
      writeLines("OutcomeAsValueDomain should be 'Measurement' or 'Observation' ")       
    }else if (length(OutcomeAsValueConceptId)==0) {
      writeLines('ConceptId should be selected!(should be vector of int)')
      
    }else if (OutcomeAsValueDomain == 'Measurement') {
      writeLines("Extracting values... : From Measurement table") 
      
      ValueDf <- querySql(conn,sprintf("SELECT measurement_id as Measurementid, PERSON_ID as subjectId, measurement_date as ValueDate, Measurement_DATETIME as ValueDateTime, value_as_number as VALUEASNUMBER, range_low as RANGELOW, range_high as RANGEHIGH  FROM %s.measurement where value_as_number is not null and measurement_concept_id in (%s)",cdmDatabaseSchema, VectorOfConcepts)) # extract every lab values
      writeLines("Extracting values... : Done")
    } else if (OutcomeAsValueDomain == 'Observation'){
      writeLines("Extracting values...: from Observation table...")                   
      ValueDf <- querySql(conn,sprintf("SELECT Observation_id as Observationid, PERSON_ID as subjectId, Observation_date as ObservationDate, Observation_DATETIME as ObservationDateTime, value_as_number as VALUEASNUMBER, range_low as RANGELOW, range_high as RANGEHIGH  FROM %s.observation where value_as_number is not null and Observation_concept_id in (%s)",cdmDatabaseSchema, VectorOfConcepts))
      writeLines("Extracting values... : Done")
    }
    writeLines(sprintf('ValueDF : %s',nrow(ValueDf)))
    
    # if ValueDf was made...merge with ECGs
    if  (exists('ValueDf')){
      writeLines('Merging values with PopulationDf : ')
      PopulationValueDf <- inner_join(ValueDf, PopulationDf, by = c('SUBJECTID'='subjectId'))
      
      writeLines(sprintf('Filtering : IndexDate + %s < MeasurementDate <IndexDate + %s',OutcomeAsValueStartDays, OutcomeAsValueEndDays))
      PopulationValueDf$ValueDaysAfterIndex <- PopulationValueDf$VALUEDATE - PopulationValueDf$cohortStartDate
      PopulationValueDfFiltered <- PopulationValueDf %>% filter(ValueDaysAfterIndex >= OutcomeAsValueStartDays) %>% filter(ValueDaysAfterIndex <= OutcomeAsValueEndDays) %>% arrange(SUBJECTID, VALUEDATETIME) 
      writeLines('Filtering : Done!')
      
      InterPath=strsplit(OutputPath,'.csv')[[1]]
      
      write.csv(PopulationValueDfFiltered, sprintf('%s_inter.csv',InterPath))
      writeLines(sprintf('Writing metadata of Joined value-population to "%s" ', sprintf('%s_inter.csv',InterPath)))
      
      writeLines(sprintf('Aggregating values : %s ',OutcomeAsValueAggregation))
      if (!OutcomeAsValueAggregation %in% c( 'Mean', 'Median','Max','Min','Nearest','Farthest')) {
        writeLines("Aggregation method should be 'Mean','Median','Max','Min','Nearest' or 'Farthest' ")
      } 
      else if (OutcomeAsValueAggregation=='Mean') {
        GroupValueDf <-  PopulationValueDfFiltered %>% group_by(rowId) %>% summarise(VALUEASNUMBER = mean(VALUEASNUMBER)) 
      } 
      else if (OutcomeAsValueAggregation=='Median') {
        GroupValueDf <- PopulationValueDfFiltered %>% group_by(rowId) %>% summarise(VALUEASNUMBER = median(VALUEASNUMBER)) 
      }
      else if (OutcomeAsValueAggregation=='Max') {
        GroupValueDf <- PopulationValueDfFiltered %>% group_by(rowId) %>% summarise(VALUEASNUMBER = max(VALUEASNUMBER)) 
      }
      else if (OutcomeAsValueAggregation=='Min') {
        GroupValueDf <- PopulationValueDfFiltered %>% group_by(rowId) %>% summarise(VALUEASNUMBER = min(VALUEASNUMBER)) 
      } 
      else if (OutcomeAsValueAggregation=='Nearest') {
        GroupValueDf <-  PopulationValueDfFiltered %>% arrange(rowId, abs(ValueDaysAfterIndex)) %>% group_by(rowId) %>% filter(row_number() ==1 )
        GroupValueDf <- GroupValueDf[,c('rowId','VALUEASNUMBER', 'RANGELOW', 'RANGEHIGH')]
      }
      else if (OutcomeAsValueAggregation=='Farthest') {
        GroupValueDf <-  PopulationValueDfFiltered %>% arrange(rowId, abs(ValueDaysAfterIndex)) %>% group_by(rowId) %>% filter(row_number() ==n() )
        GroupValueDf <- GroupValueDf[,c('rowId','VALUEASNUMBER', 'RANGELOW', 'RANGEHIGH')]}
      writeLines('Aggregating values : Done ')
      
      writeLines(sprintf('Number of entrys which have proper value data : %s.',nrow(GroupValueDf)))
      writeLines('Entrys without proper value data will have Null value in outcome column')
      
      PopWithOutcomeValue<-left_join(PopulationDf,GroupValueDf,by = c('rowId'='rowId')) 
      PopulationDf <- PopWithOutcomeValue %>% select(-outcomeCount) %>% rename(outcomeCount = VALUEASNUMBER)
      
      writeLines('Using value(measurement or observation) as outcome : Done')
############################################################      
      if (!identical(OutcomeAsValueAsRange,FALSE)) {
        if (!OutcomeAsValueAsRange %in% c( 'Hyper', 'Hypo','Bothside')) {
          writeLines("Aggregation method should be 'Hyper','Hypo' or 'Bothside'")
        }
        else if (OutcomeAsValueAsRange=='Hyper'){
          outofrange <- ifelse(PopulationDf$outcomeCount> PopulationDf$RANGEHIGH, 1, 0)
          PopulationDf$outcomeCount <- outofrange
        }
        else if (OutcomeAsValueAsRange=='Hypo'){
          outofrange <- ifelse(PopulationDf$outcomeCount< PopulationDf$RANGELOW, 1, 0)
          PopulationDf$outcomeCount <- outofrange
        }
        else if (OutcomeAsValueAsRange=='Bothside'){
          outofrange <- ifelse((PopulationDf$outcomeCount< PopulationDf$RANGELOW) |(PopulationDf$outcomeCount> PopulationDf$RANGEHIGH), 1, 0)
          PopulationDf$outcomeCount <- outofrange
        }
        
      }
      
    } else {writeLines("Value data don't exist")}}
  
  writeLines('Extracting ECG records from CDM database')
  ECGsDf <- querySql(conn,sprintf("SELECT OBSERVATION_ID as Observationid, PERSON_ID as subjectId, OBSERVATION_DATE as ECGDate, OBSERVATION_DATETIME as ECGDateTime, OBSERVATION_SOURCE_VALUE as XMLPath FROM %s.observation where observation_concept_id =40761354",cdmDatabaseSchema))
  writeLines('Extracting ECG records...:Done')
  MergedDf <- inner_join(ECGsDf, PopulationDf, by = c('SUBJECTID'='subjectId'))
  MergedDf$EcgDaysBeforeIndex <- MergedDf$ECGDATE - MergedDf$cohortStartDate
  writeLines(sprintf('Filtering : IndexDate + %s < ECGDate <IndexDate + %s',EcgStartDaysBeforeIndex, EcgEndDaysBeforeIndex))
  MergedDfFiltered <- MergedDf %>% filter(EcgDaysBeforeIndex >= EcgStartDaysBeforeIndex) %>% filter(EcgDaysBeforeIndex <= EcgEndDaysBeforeIndex) 
  OrderedDf <- MergedDfFiltered %>% arrange(SUBJECTID,ECGDATETIME)
  writeLines('Filtering : Done')#Ordered by SubjectId, ECGdatetime
  writeLines(sprintf('Number of proper ECGs : %s.',nrow(OrderedDf)))
  
  writeLines(sprintf('EcgAggregation : %s',EcgAggregation))
  if (!EcgAggregation %in% c('All','Farthest','Nearest')) {
    writeLines("ECG Aggregation method should be 'All' or 'Farthest' or 'Nearest' ")
  } 
  else if (EcgAggregation=='All') {
    FinalDf <- OrderedDf
    FinalDf <- unique(na.omit(FinalDf))
    write.csv(FinalDf, OutputPath)
  } 
  else if (EcgAggregation=='Nearest') {
    FinalDf <-   OrderedDf %>% arrange(rowId, abs(EcgDaysBeforeIndex))%>% group_by(rowId) %>% filter(row_number() ==1 )
    FinalDf <- unique(na.omit(FinalDf))
    write.csv(FinalDf, OutputPath)
  }
  else if (EcgAggregation=='Farthest') {
    FinalDf <-   OrderedDf%>% arrange(rowId, abs(EcgDaysBeforeIndex))  %>% group_by(rowId) %>% filter(row_number() ==n() )
    FinalDf <- unique(na.omit(FinalDf))
    write.csv(FinalDf, OutputPath)
  } 
  writeLines('EcgAggregation : Done')
  
  writeLines(sprintf('Number of entrys which have proper ECG data : %s.',nrow(FinalDf)))
  writeLines(sprintf('Number of patients which have proper ECG data : %s.',length(unique(FinalDf$SUBJECTID))))
  
  if (!identical(OutcomeAsValue,FALSE)){
    FinalDfNotNa<- FinalDf %>% filter(!is.na(outcomeCount))
    writeLines(sprintf('Number of entrys which have proper ECG data and Value data(Outcome is not null) : %s.',nrow(FinalDfNotNa)))
    writeLines(sprintf('Number of patients which have proper ECG data and Value data(Outcome is not null) : %s.',length(unique(FinalDfNotNa$SUBJECTID))))
  }
  writeLines(sprintf('Saving populations with ECG records : %s',OutputPath))
  FinalDf <- unique(na.omit(FinalDf)) # Remove duplicates
  return(FinalDf) 
} 

ECG_measurement_pair <- read.csv("/home/cbj/datahouse/sejong/ECG_measurement_pair.csv", stringsAsFactors=FALSE)

for(i in ECG_measurement_pair$measurement_concept_id){
  FinalDf<-get_TargetEcgsWithOutcome(PopulationPath='StudyPop_L1_T221_O225_P1.rds',
                                     OutputPath=sprintf('/home/cbj/datahouse/sejong/Screening/%s_with_ECGs.csv', i),
                                     OutcomeAsValue=TRUE, 
                                     OutcomeAsValueDomain='Measurement', 
                                     OutcomeAsValueConceptId=c(i),
                                     OutcomeAsValueStartDays=-7,
                                     OutcomeAsValueEndDays=7, 
                                     OutcomeAsValueAggregation='Nearest',
                                     OutcomeAsValueAsRange='Hyper',
                                     EcgStartDaysBeforeIndex=0, 
                                     EcgEndDaysBeforeIndex=0, 
                                     EcgAggregation='All' 
  )
}
# Uncomment and run the next line to see the shiny results:

# Uncomment and run the next line to see the shiny results:
# PatientLevelPrediction::viewMultiplePlp(outputFolder)
