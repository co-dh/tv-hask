# Rename Table: Loud Names → Max 2 Words

## Record fields — strip redundant type prefix

### CmdConfig.hs — Entry (strip `entry`)

| Old              | New       |
|------------------|-----------|
| `entryCmd`       | `cmd`     |
| `entryCtx`       | `ctx`     |
| `entryKey`       | `key`     |
| `entryLabel`     | `label`   |
| `entryResetsVS`  | `resets`  |
| `entryViewCtx`   | `viewCtx` |

### CmdConfig.hs — CmdInfo (strip `ci`; needs DuplicateRecordFields)

| Old          | New      |
|--------------|----------|
| `ciCmd`      | `cmd`    |
| `ciResetsVS` | `resets` |

### Adbc.hs — QueryResult (strip `qr`)

| Old          | New        |
|--------------|------------|
| `qrChunks`   | `chunks`   |
| `qrOffsets`  | `offsets`  |
| `qrNRows`    | `nRows`    |
| `qrColNames` | `colNames` |
| `qrColTypes` | `colTypes` |

### Term.hs — Event (strip `event`)

| Old            | New       |
|----------------|-----------|
| `eventType`    | `typ`     |
| `eventMod`     | `mods`    |
| `eventKeyCode` | `keyCode` |
| `eventCh`      | `ch`      |
| `eventW`       | `w`       |
| `eventH`       | `h`       |

## Functions — by file

### Types.hs

| Old                | New          |
|--------------------|--------------|
| `colTypeStr`       | `typeStr`    |
| `colTypeOfString`  | `ofString`   |
| `colTypeIsNumeric` | `isNumeric`  |
| `colTypeIsTime`    | `isTime`     |
| `isNumericType`    | `numType`    |
| `cellToRaw`        | `toRaw`      |
| `cellToPrql`       | `toPrql`     |
| `freqPctBar`       | `pctBar`     |
| `buildFilterPrql`  | `filterPrql` |
| `colsToText`       | `colText`    |
| `aggFromStrQ`      | `parseAgg`   |
| `viewKindCtxStr`   | `vkindStr`   |
| `effectIsNone`     | `noEffect`   |
| `cmdPlotKindQ`     | `plotKind`   |

### Nav.hs

| Old              | New         |
|------------------|-------------|
| `navAxisDefault` | `defAxis`   |
| `colIdxAt`       | `idxAt`     |
| `curColIdx`      | `colIdx`    |
| `curColName`     | `colName`   |
| `curColType`     | `colType`   |
| `dispColNames`   | `dispNames` |
| `selColIdxs`     | `selIdxs`   |
| `rowCurL`        | `rowCur`    |
| `colCurL`        | `colCur`    |
| `rowSelsL`       | `rowSels`   |

### Render.hs

| Old                | New       |
|--------------------|-----------|
| `viewStateDefault` | `defVS`   |
| `maxVisRows`       | `visRows` |
| `defaultRowPg`     | `rowPg`   |
| `renderTabLine`    | `tabLine` |

### Session.hs

| Old             | New        |
|-----------------|------------|
| `aggToJson`     | `aggToJ`   |
| `aggFromJson`   | `aggFromJ` |
| `opToJson`      | `opToJ`    |
| `opFromJson`    | `opFromJ`  |
| `vkindToJson`   | `vkToJ`    |
| `vkindFromJson` | `vkFromJ`  |
| `stackToJson`   | `stkToJ`   |
| `getObjVal`     | `objVal`   |
| `getObjValD`    | `objValD`  |
| `pickSaveName`  | `saveName` |
| `pickLoadName`  | `loadName` |

### SourceConfig.hs (strip `config`)

| Old                   | New           |
|-----------------------|---------------|
| `configCmdVars`       | `cmdVars`     |
| `configAttachSql`     | `attachSql`   |
| `configRunListSql`    | `runListSql`  |
| `configRunListCmd`    | `runListCmd`  |
| `configRunList`       | `runList`     |
| `configRunDownload`   | `runDl`       |
| `configRunEnter`      | `runEnter`    |
| `extdbFilteredSql`    | `filteredSql` |
| `getExtdbFilteredSql` | `getFiltSql`  |
| `setNoSign`           | `setNS`       |
| `getNoSign`           | `getNS`       |
| `hasShellMeta`        | `shellMeta`   |
| `validateShellSafe`   | `checkShell`  |
| `nameFromPath`        | `fromPath`    |

### Table.hs (Data/ADBC/Table.hs)

| Old                 | New           |
|---------------------|---------------|
| `memTblCounter`     | `tblCounter`  |
| `nextTmpName`       | `tmpName`     |
| `loadDuckExt`       | `loadExt`     |
| `ofQueryResult`     | `ofResult`    |
| `fromTmpTbl`        | `fromTmp`     |
| `remoteTblName`     | `remoteName`  |
| `listDuckDBTables`  | `listTables`  |
| `duckDBPrimaryKeys` | `primaryKeys` |
| `fromDuckDBTable`   | `fromTable`   |
| `fromFileWith`      | `fromFile`    |

### DuckDB.hs (strip `read`)

| Old                | New        |
|--------------------|------------|
| `readCellInt`      | `cellInt`  |
| `readCellDouble`   | `cellDbl`  |
| `readCellText`     | `cellText` |
| `readCellAny`      | `cellAny`  |
| `readCellDate`     | `cellDate` |
| `readCellTime`     | `cellTime` |
| `readCellTimestamp` | `cellTs`   |

### Data/Text.hs

| Old               | New          |
|-------------------|--------------|
| `countWordStarts` | `wordStarts` |
| `findColStarts`   | `colStarts`  |
| `splitByStarts`   | `splitAt'`   |

### App/Common.hs

| Old             | New       |
|-----------------|-----------|
| `curPrecL`      | `precL`   |
| `runStackIO`    | `stackIO` |
| `viewCtxStr`    | `ctxStr`  |
| `threadDelayMs` | `delayMs` |

### App/Main.hs

| Old              | New       |
|------------------|-----------|
| `defaultCliArgs` | `defArgs` |

### Remaining files (1-3 each)

| File         | Old                 | New          |
|--------------|---------------------|--------------|
| Diff.hs      | `renameDiffCols`    | `renameCols` |
| FileFormat.hs| `isDataFile`        | `isData`     |
| FileFormat.hs| `isTxtFile`         | `isTxt`      |
| FileFormat.hs| `tryReadCsv`        | `readCsv`    |
| Filter.hs    | `moveColTo`         | `moveTo`     |
| Filter.hs    | `colJumpWith`       | `jumpCol`    |
| Folder.hs    | `pathColIdx`        | `pathIdx`    |
| Folder.hs    | `mkFldView`         | `fldView`    |
| Ftp.hs       | `urlEncodeUrl`      | `encodeUrl`  |
| Fzf.hs       | `setTestMode`       | `setTest`    |
| Fzf.hs       | `getTestMode`       | `getTest`    |
| Fzf.hs       | `parseFlatSel`      | `parseSel`   |
| Key.hs       | `evToKey`           | `toKey`      |
| Meta.hs      | `metaTblName`       | `tblName`    |
| Plot.hs      | `enterAltScreen`    | `altEnter`   |
| Plot.hs      | `leaveAltScreen`    | `altLeave`   |
| Plot.hs      | `exitPlotMode`      | `exitPlot`   |
| Plot.hs      | `readKeyRaw`        | `readKey`    |
| Plot.hs      | `isSingleColPlot`   | `singleCol`  |
| Plot.hs      | `usesCategoryAsX`   | `catAsX`     |
| Runner.hs    | `filterExprIO`      | `filterIO`   |
| Theme.hs     | `stateApplyIdx`     | `applyIdx`   |
| Term.hs      | `queryTermSize`     | `termSize`   |
| Term.hs      | `byteToEvent`       | `toEvent`    |
| Term.hs      | `bytesToEvent`      | `toEvents`   |
| Term.hs      | `printPadC`         | `padC`       |
| Util.hs      | `setLogPath`        | `setLog`     |
| Util.hs      | `tryRemoveFile`     | `rmFile`     |
| Util.hs      | `sockBufRef`        | `bufRef`     |
| Util.hs      | `sockListenSock`    | `listener`   |
| Util.hs      | `sockPollCmd`       | `pollCmd`    |
| Util.hs      | `setEnvVar`         | `setEnv`     |
| AppF.hs      | `bindAppM`          | `bindApp`    |
| Ops.hs       | `parquetMetaPrql`   | `metaPrql`   |
| Ops.hs       | `colStatsSql`       | `statsSql`   |
| Ops.hs       | `queryMetaIndices`  | `metaIdxs`   |
| Ops.hs       | `queryMetaColNames` | `metaNames`  |
| Prql.hs      | `queryRenderOps`    | `renderOps`  |

## Notes

- CmdConfig.hs needs `DuplicateRecordFields` for `cmd`/`resets` overlap between Entry and CmdInfo
- No `#label` optics references exist for any of the prefixed record fields
- ~120 renames total
