VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
  Persistable = 0  'NotPersistable
  DataBindingBehavior = 0  'vbNone
  DataSourceBehavior  = 0  'vbNone
  MTSTransactionMode  = 0  'NotAnMTSObject
END
Attribute VB_Name = "clsForecast"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = True
Attribute VB_PredeclaredId = False
Attribute VB_Exposed = False
Option Explicit

'Old: Public Function ForecastPattern(l_Values() As String, l_PatternLength As Long, l_Horizon As Long, l_Precision As Double) As String
Public Function ForecastPattern(l_Value As String, l_HistoryLength As Long, l_PatternLength As Long, l_Horizon As Long, l_Precision As Double) As String
'New:
On Error GoTo err_handler
Static Data()       As String
'New:
Static HistorySize  As Long
'New:
Dim Price           As String
Dim DataPart()      As Double
'Old: Dim IsInitialized   As Long
Static IsInitialized As Boolean
Dim i               As Long
Dim i2              As Long
Dim NoOfNewValues   As Long
Dim NoOfPositives   As Long
Dim NoOfNegatives   As Long
Dim NoOfNoChange    As Long
Dim NoOfAll         As Long
Dim AvgChgPositive  As Double
Dim AvgChgNegative  As Double
Dim Pattern()       As Double
Dim Correlation     As Double
Dim JSON_Parts()    As String

'Check input data
'Old: If UBound(l_Values) = -1 Then
If l_Value = "" Then
    ForecastPattern = "No input data"
    Exit Function
End If
If l_PatternLength < 2 Then
    ForecastPattern = "PatternLength must be bigger than 1"
    Exit Function
End If
If l_Horizon < 1 Then
    ForecastPattern = "Horizon must be bigger than 0"
    Exit Function
End If
If l_Precision < 0 Or l_Precision > 1 Then
    ForecastPattern = "Precision needs to be between 0 and 1"
    Exit Function
End If

'New:
If HistorySize = 0 Then
    If l_HistoryLength <= 0 Then
        ForecastPattern = "HistoryLength must be bigger than 0"
        Exit Function
    Else
        HistorySize = l_HistoryLength
    End If
End If

'New: Extract "price" value from JSON string
JSON_Parts = Split(l_Value, Chr(34))
For i = 0 To UBound(JSON_Parts) - 2
    If JSON_Parts(i) = "price" Then
        Price = JSON_Parts(i + 2)
        Exit For
    End If
Next i
If Price = "" Then
    ForecastPattern = "No price data"
    Exit Function
End If


'Check, if Data is initialized

'Old: IsInitialized = UBound(Data)
'Old: If IsInitialized = -1 Or IsInitialized = 0 Then
'New: Data is not initialized. In this case, Dim Data and set first value
If Not IsInitialized Then
    IsInitialized = True
    ReDim Data(HistorySize)
    Data(UBound(Data)) = Price
Else
    'Data is initialized. In this case, update Data with l_Values
    'Old: NoOfNewValues = UBound(l_Values) + 1
    NoOfNewValues = 1
    '1: Remove oldest value(s)
    For i = NoOfNewValues To UBound(Data)
        Data(i - NoOfNewValues) = Data(i)
    Next i
    '2: Add new value(s)
    'Old: For i = 0 To UBound(l_Values)
    'Old:    Data(UBound(Data) - UBound(l_Values) + i) = l_Values(i)
    'Old: Next i
    Data(UBound(Data)) = Price
End If

'New: Calc only, when array is filled completely
If Data(0) <> "" Then
    'Init Pattern
    ReDim Pattern(l_PatternLength - 1)
    For i = 0 To UBound(Pattern)
        Pattern(i) = Val(Data(UBound(Data) - UBound(Pattern) + i))
    Next i
    
    'Loop through Data and check for Pattern
    For i = 0 To UBound(Data) - l_PatternLength - l_Horizon
        'Set Data part
        ReDim DataPart(UBound(Pattern))
        For i2 = 0 To UBound(Pattern)
            DataPart(i2) = Val(Data(i + i2))
        Next i2
        'Check correlation between pattern and data part
        Correlation = PearsonCorrelation(DataPart, Pattern)
        
        'If correlation is high enough, it is a found patten
        If Correlation >= l_Precision Then
            'Is Close at Horizon higher or lower than last close ?
            If Data(i + UBound(Pattern) + l_Horizon) > Data(i + UBound(Pattern)) Then
                NoOfPositives = NoOfPositives + 1
                AvgChgPositive = AvgChgPositive + ((Data(i + UBound(Pattern) + l_Horizon) / Data(i + UBound(Pattern))) - 1) * 100
            ElseIf Data(i + UBound(Pattern) + l_Horizon) < Data(i + UBound(Pattern)) Then
                NoOfNegatives = NoOfNegatives + 1
                AvgChgNegative = AvgChgNegative + (1 - (Data(i + UBound(Pattern) + l_Horizon) / Data(i + UBound(Pattern)))) * 100
            Else
                NoOfNoChange = NoOfNoChange + 1
            End If
        End If
    Next i
    
    'Calc average change values
    If NoOfPositives > 0 Then
        AvgChgPositive = AvgChgPositive / NoOfPositives
    End If
    If NoOfNegatives > 0 Then
        AvgChgNegative = AvgChgNegative / NoOfNegatives
    End If
    
    NoOfAll = NoOfPositives + NoOfNegatives + NoOfNoChange
    
    'ForecastPattern=No of occurances; % Higher; % Lower; Avg Change% higher;  Avg Change% lower
    'New:
    If NoOfAll > 0 Then
        ForecastPattern = Str(NoOfAll) & ";" & _
                          Str(Round(NoOfPositives / NoOfAll * 100, 2)) & ";" & _
                          Str(Round(NoOfNegatives / NoOfAll * 100, 2)) & ";" & _
                          Str(Round(AvgChgPositive, 4)) & ";" & _
                          Str(Round(AvgChgNegative, 4))
    End If
End If

'New
err_handler:
If Err.Number <> 0 Then
    ForecastPattern = Err.Description
End If
End Function


Public Function PearsonCorrelation(ByRef X() As Double, ByRef Y() As Double) As Double
On Error Resume Next
Dim Result  As Double
Dim i       As Long
Dim XMean   As Double
Dim YMean   As Double
Dim S       As Double
Dim XV      As Double
Dim YV      As Double
Dim T1      As Double
Dim T2      As Double
Dim l_N     As Long

l_N = UBound(X) + 1
If l_N <= 1 Then
    PearsonCorrelation = 0
    Exit Function
End If

' Mean
For i = 0 To UBound(X)
    XMean = XMean + X(i)
    YMean = YMean + Y(i)
Next i
XMean = XMean / l_N
YMean = YMean / l_N

' numerator and denominator
For i = 0 To UBound(X)
    T1 = X(i) - XMean
    T2 = Y(i) - YMean
    XV = XV + T1 * T1
    YV = YV + T2 * T2
    S = S + T1 * T2
Next i
If XV = 0 Or YV = 0 Then
    Result = 0
Else
    Result = S / (Sqr(XV) * Sqr(YV))
End If

PearsonCorrelation = Result

End Function




