<#
Centralized SSRS report constants so paths & parameter names live in one place.

Usage:
  # Dot-source this early in a script
  . "$PSScriptRoot/ReportConstants.ps1"
  # Then refer to $Global:REPORT_CONSTANTS hashtable.

Structure:
  $Global:REPORT_CONSTANTS = @{
    ReportServerRoot = 'http://c201m580/ReportServer'
    SchedulingSummary = @{ ReportPath='/Custom/Production Control/SCHSchedulingSummaryReport'; StartParamName='SHIP_DATE_START'; EndParamName='SHIP_DATE_END' }
    Labor = @{ ReportPath='/Custom/Production Control/SCHLabor'; StartParamName='DateStart'; EndParamName='DateEnd' }
  }

Adjust here if the server, paths, or param names ever change.
#>

if(-not $Global:REPORT_CONSTANTS){
  $Global:REPORT_CONSTANTS = @{
    ReportServerRoot     = 'http://c201m580/ReportServer'
    SchedulingSummary    = @{ ReportPath='/Custom/Production Control/SCHSchedulingSummaryReport'; StartParamName='SHIP_DATE_START'; EndParamName='SHIP_DATE_END' }
    Labor                = @{ ReportPath='/Custom/Production Control/SCHLabor'; StartParamName='DateStart'; EndParamName='DateEnd' }
  }
}
