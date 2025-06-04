# LAND USE API

## Main info
This API is created to calculate the level of urbanization and renovation potential of land use zones.

## API methods
### /api/projects/{project_id}/renovation_potential
Endpoint for renovation potential calculation for given project ID
### /api/projects/{project_id}/urbanization_level
Endpoint for urbanization level calculation for given project ID
### /api/projects/{project_id}/context/renovation_potential
Endpoint for renovation potential calculation for context of given project ID
### /api/projects/{project_id}/context/urbanization_level
Endpoint for renovation potential calculation for context of given project ID
### /api/scenarios/{scenario_id}/landuse_percentages
Endpoint for getting land use percentages for a scenario.
## Indicators
### /api/indicators/{territory_id}/calculate_territory_urbanization
Endpoint for urbanization percentage indicator calculation for given territory ID
### /api/indicators/{territory_id}/calculate_area_indicator
Endpoint for area indicator calculation for given territory ID
### /api/indicators/{territory_id}/services_count_indicator
Endpoint for services counts indicators calculation and for given territory ID
### /api/indicators/{project_id}/calculate_project_area_indicator
Endpoint for services counts indicators calculation and for given project ID
## System
### /health_check/ping
Endpoint for application work ping
### /logs
Endpoint for getting logs