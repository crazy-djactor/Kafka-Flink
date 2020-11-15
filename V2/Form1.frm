VERSION 5.00
Begin VB.Form Form1 
   Caption         =   "Form1"
   ClientHeight    =   4950
   ClientLeft      =   60
   ClientTop       =   405
   ClientWidth     =   4680
   LinkTopic       =   "Form1"
   ScaleHeight     =   4950
   ScaleWidth      =   4680
   StartUpPosition =   3  'Windows Default
   Begin VB.TextBox Text6 
      Height          =   2415
      Left            =   120
      MultiLine       =   -1  'True
      ScrollBars      =   2  'Vertical
      TabIndex        =   12
      Top             =   2400
      Width           =   4455
   End
   Begin VB.TextBox Text5 
      Height          =   285
      Left            =   1440
      TabIndex        =   11
      Text            =   "0.95"
      Top             =   2040
      Width           =   1095
   End
   Begin VB.TextBox Text4 
      Height          =   285
      Left            =   1440
      TabIndex        =   10
      Text            =   "5"
      Top             =   1680
      Width           =   1095
   End
   Begin VB.TextBox Text3 
      Height          =   285
      Left            =   1440
      TabIndex        =   9
      Text            =   "10"
      Top             =   1320
      Width           =   1095
   End
   Begin VB.TextBox Text2 
      Height          =   285
      Left            =   1440
      TabIndex        =   8
      Text            =   "1"
      Top             =   960
      Width           =   1095
   End
   Begin VB.TextBox Text1 
      Height          =   285
      Left            =   1440
      TabIndex        =   7
      Text            =   "3000"
      Top             =   600
      Width           =   1095
   End
   Begin VB.CommandButton Command1 
      Caption         =   "Calc"
      Height          =   375
      Left            =   2760
      TabIndex        =   0
      Top             =   1920
      Width           =   1095
   End
   Begin VB.Label Label6 
      Caption         =   "Precision"
      Height          =   255
      Left            =   120
      TabIndex        =   6
      Top             =   2040
      Width           =   1215
   End
   Begin VB.Label Label5 
      Caption         =   "Forecast horizon"
      Height          =   255
      Left            =   120
      TabIndex        =   5
      Top             =   1680
      Width           =   1215
   End
   Begin VB.Label Label4 
      Caption         =   "Pattern length"
      Height          =   255
      Left            =   120
      TabIndex        =   4
      Top             =   1320
      Width           =   1215
   End
   Begin VB.Label Label3 
      Caption         =   "Step"
      Height          =   255
      Left            =   120
      TabIndex        =   3
      Top             =   960
      Width           =   1215
   End
   Begin VB.Label Label2 
      Caption         =   "Data length"
      Height          =   255
      Left            =   120
      TabIndex        =   2
      Top             =   600
      Width           =   1215
   End
   Begin VB.Label Label1 
      Caption         =   "Data from ""/data_JSON.txt""      Results to /""result.txt"""
      Height          =   255
      Left            =   120
      TabIndex        =   1
      Top             =   120
      Width           =   4455
   End
End
Attribute VB_Name = "Form1"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = True
Attribute VB_Exposed = False
Option Explicit

Private Sub Command1_Click()
'Test the forecast function
Dim f           As Integer
Dim i           As Long
Dim i2          As Long
Dim l_Size      As Long
Dim l_Str       As String
Dim l_ArrAll()  As String
'Old: Dim l_ArrPart() As String
'New:
Dim l_Value     As String
Dim l_Result    As String
Dim l_Forecast  As New clsForecast

'Get data from file
f = FreeFile
Open App.Path & "/data_JSON.txt" For Binary Shared As f
      l_Size = LOF(f)
      l_Str = Space(l_Size)
      Get #f, 1, l_Str
Close f
l_ArrAll = Split(l_Str, vbCrLf)

'Kill result file
If Dir(App.Path & "/result.txt") <> "" Then
    Kill App.Path & "/result.txt"
End If

'Prepare result file
f = FreeFile
Open App.Path & "/result.txt" For Append As f


'Loop through data
'Old: For i = 0 To UBound(l_ArrAll) - Val(Text1.Text) Step Val(Text2.Text)  'The length of analysis data
For i = 0 To UBound(l_ArrAll) Step Val(Text2.Text)  'The length of analysis data
    'Old: ReDim l_ArrPart(Val(Text1.Text) - 1)
    'Old: For i2 = 0 To Val(Text1.Text) - 1
    'Old:    l_ArrPart(i2) = l_ArrAll(i + i2)
    'Old Next i2
    'New:
    l_Value = l_ArrAll(i)
    'Old: l_Result = l_Forecast.ForecastPattern(l_ArrPart, Val(Text3.Text), Val(Text4.Text), Val(Text5.Text))
    l_Result = l_Forecast.ForecastPattern(l_Value, Val(Text1.Text), Val(Text3.Text), Val(Text4.Text), Val(Text5.Text))
    Print #f, l_Result
Next i

Close f
End Sub
