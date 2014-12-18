import AssemblyKeys._ // put this at the top of the file

assemblySettings

val defaultMergeStrategy: String => MergeStrategy = {
    case _ => MergeStrategy.first
}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case m if m.toLowerCase.matches("meta-inf/.*\\.sf$") => MergeStrategy.discard
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
}
