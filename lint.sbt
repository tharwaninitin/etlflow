ThisBuild / wartremoverErrors ++= Warts.allBut(
  Wart.Any,
  Wart.DefaultArguments,
  Wart.Nothing,
  Wart.Equals,
  Wart.FinalCaseClass,
  Wart.Overloading,
  Wart.StringPlusAny
)
