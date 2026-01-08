// RULE 1: EXACT MATCH
  console.log("Rule 1: Exact matching...");
  bankTransactions.forEach(bankTxn => {
    if (bankTxn.matched) return;
    
    const exactMatch = ledgerTransactions.find(ledgerTxn => 
      !ledgerTxn.matched &&
      ledgerTxn.date === bankTxn.date &&
      Math.abs(ledgerTxn.amount - bankTxn.amount) < 0.01 &&
      ledgerTxn.type === bankTxn.type
    );
    
    if (exactMatch) {
      bankTxn.matched = true;
      exactMatch.matched = true;
      bankTxn.matchedWith = [exactMatch.id];
      exactMatch.matchedWith = [bankTxn.id];
      bankTxn.matchType = 'Exact Match';
      exactMatch.matchType = 'Exact Match';
      bankTxn.matchScore = 100;
      exactMatch.matchScore = 100;
      
      matched.push({
        matchType: 'Exact Match',
        confidence: 100,
        bankTxn,
        ledgerTxns: [exactMatch]
      });
    }
  });
  console.log(`Exact matches: ${matched.length}`);

  // RULE 2: FUZZY DATE MATCH
  console.log("Rule 2: Fuzzy date matching...");
  bankTransactions.forEach(bankTxn => {
    if (bankTxn.matched) return;
    
    const bankDate = bankTxn.date ? new Date(bankTxn.date) : null;
    if (!bankDate || isNaN(bankDate.getTime())) return;
    
    const fuzzyMatch = ledgerTransactions.find(ledgerTxn => {
      if (ledgerTxn.matched) return false;
      
      const ledgerDate = ledgerTxn.date ? new Date(ledgerTxn.date) : null;
      if (!ledgerDate || isNaN(ledgerDate.getTime())) return false;
      
      const daysDiff = Math.abs((bankDate - ledgerDate) / (1000 * 60 * 60 * 24));
      
      return daysDiff <= 5 &&
             Math.abs(ledgerTxn.amount - bankTxn.amount) < 0.01 &&
             ledgerTxn.type === bankTxn.type;
    });
    
    if (fuzzyMatch) {
      const daysDiff = Math.abs((bankDate - new Date(fuzzyMatch.date)) / (1000 * 60 * 60 * 24));
      const confidence = Math.round(95 - (daysDiff * 3));
      
      bankTxn.matched = true;
      fuzzyMatch.matched = true;
      bankTxn.matchedWith = [fuzzyMatch.id];
      fuzzyMatch.matchedWith = [bankTxn.id];
      bankTxn.matchType = `Fuzzy Date Match (${Math.round(daysDiff)} days)`;
      fuzzyMatch.matchType = `Fuzzy Date Match (${Math.round(daysDiff)} days)`;
      bankTxn.matchScore = confidence;
      fuzzyMatch.matchScore = confidence;
      
      matched.push({
        matchType: `Fuzzy Date Match (${Math.round(daysDiff)} days)`,
        confidence,
        bankTxn,
        ledgerTxns: [fuzzyMatch]
      });
    }
  });
  console.log(`After fuzzy date matching: ${matched.length}`);

  // RULE 3: AMOUNT TOLERANCE
  console.log("Rule 3: Amount tolerance matching...");
  bankTransactions.forEach(bankTxn => {
    if (bankTxn.matched) return;
    
    const tolerance = bankTxn.amount * 0.005;
    
    const amountMatch = ledgerTransactions.find(ledgerTxn => 
      !ledgerTxn.matched &&
      Math.abs(ledgerTxn.amount - bankTxn.amount) <= tolerance &&
      ledgerTxn.type === bankTxn.type
    );
    
    if (amountMatch) {
      const diff = Math.abs(amountMatch.amount - bankTxn.amount);
      const pctDiff = (diff / bankTxn.amount) * 100;
      const confidence = Math.round(90 - (pctDiff * 10));
      
      bankTxn.matched = true;
      amountMatch.matched = true;
      bankTxn.matchedWith = [amountMatch.id];
      amountMatch.matchedWith = [bankTxn.id];
      bankTxn.matchType = `Amount Match (${pctDiff.toFixed(2)}% diff)`;
      amountMatch.matchType = `Amount Match (${pctDiff.toFixed(2)}% diff)`;
      bankTxn.matchScore = confidence;
      amountMatch.matchScore = confidence;
      
      matched.push({
        matchType: `Amount Match (${pctDiff.toFixed(2)}% diff)`,
        confidence,
        bankTxn,
        ledgerTxns: [amountMatch]
      });
    }
  });
  console.log(`After amount tolerance matching: ${matched.length}`);

  // RULE 4: DESCRIPTION SIMILARITY
  console.log("Rule 4: Description similarity matching...");
  bankTransactions.forEach(bankTxn => {
    if (bankTxn.matched) return;
    if (!bankTxn.description || bankTxn.description.length < 5) return;
    
    let bestMatch = null;
    let bestSimilarity = 0;
    
    ledgerTransactions.forEach(ledgerTxn => {
      if (ledgerTxn.matched) return;
      if (!ledgerTxn.description || ledgerTxn.description.length < 5) return;
      if (Math.abs(ledgerTxn.amount - bankTxn.amount) > 0.01) return;
      if (ledgerTxn.type !== bankTxn.type) return;
      
      const similarity = stringSimilarity(bankTxn.description, ledgerTxn.description);
      
      if (similarity > 0.6 && similarity > bestSimilarity) {
        bestSimilarity = similarity;
        bestMatch = ledgerTxn;
      }
    });
    
    if (bestMatch && bestSimilarity >= 0.6) {
      const confidence = Math.round(bestSimilarity * 80);
      
      bankTxn.matched = true;
      bestMatch.matched = true;
      bankTxn.matchedWith = [bestMatch.id];
      bestMatch.matchedWith = [bankTxn.id];
      bankTxn.matchType = `Description Match (${Math.round(bestSimilarity * 100)}% similar)`;
      bestMatch.matchType = `Description Match (${Math.round(bestSimilarity * 100)}% similar)`;
      bankTxn.matchScore = confidence;
      bestMatch.matchScore = confidence;
      
      matched.push({
        matchType: `Description Match (${Math.round(bestSimilarity * 100)}% similar)`,
        confidence,
        bankTxn,
        ledgerTxns: [bestMatch]
      });
    }
  });
  console.log(`After description matching: ${matched.length}`);

  // RULE 5: MANY-TO-ONE
  console.log("Rule 5: Many-to-one matching...");
  bankTransactions.forEach(bankTxn => {
    if (bankTxn.matched) return;
    
    const unmatchedLedger = ledgerTransactions.filter(l => !l.matched && l.type === bankTxn.type);
    
    for (let groupSize = 2; groupSize <= Math.min(5, unmatchedLedger.length); groupSize++) {
      const combinations = getCombinations(unmatchedLedger, groupSize);
      
      for (const combo of combinations) {
        const totalAmount = combo.reduce((sum, txn) => sum + txn.amount, 0);
        
        if (Math.abs(totalAmount - bankTxn.amount) < 0.01) {
          bankTxn.matched = true;
          bankTxn.matchedWith = combo.map(t => t.id);
          bankTxn.matchType = `Many-to-One (${combo.length} ledger entries)`;
          bankTxn.matchScore = 85;
          
          combo.forEach(ledgerTxn => {
            ledgerTxn.matched = true;
            ledgerTxn.matchedWith = [bankTxn.id];
            ledgerTxn.matchType = `Many-to-One (${combo.length} entries)`;
            ledgerTxn.matchScore = 85;
          });
          
          matched.push({
            matchType: `Many-to-One (${combo.length} ledger entries)`,
            confidence: 85,
            bankTxn,
            ledgerTxns: combo
          });
          
          break;
        }
      }
      
      if (bankTxn.matched) break;
    }
  });
  console.log(`After many-to-one matching: ${matched.length}`);

  // RULE 6: ONE-TO-MANY
  console.log("Rule 6: One-to-many matching...");
  ledgerTransactions.forEach(ledgerTxn => {
    if (ledgerTxn.matched) return;
    
    const unmatchedBank = bankTransactions.filter(b => !b.matched && b.type === ledgerTxn.type);
    
    for (let groupSize = 2; groupSize <= Math.min(5, unmatchedBank.length); groupSize++) {
      const combinations = getCombinations(unmatchedBank, groupSize);
      
      for (const combo of combinations) {
        const totalAmount = combo.reduce((sum, txn) => sum + txn.amount, 0);
        
        if (Math.abs(totalAmount - ledgerTxn.amount) < 0.01) {
          ledgerTxn.matched = true;
          ledgerTxn.matchedWith = combo.map(t => t.id);
          ledgerTxn.matchType = `One-to-Many (${combo.length} bank entries)`;
          ledgerTxn.matchScore = 85;
          
          combo.forEach(bankTxn => {
            bankTxn.matched = true;
            bankTxn.matchedWith = [ledgerTxn.id];
            bankTxn.matchType = `One-to-Many (${combo.length} entries)`;
            bankTxn.matchScore = 85;
          });
          
          matched.push({
            matchType: `One-to-Many (${combo.length} bank entries)`,
            confidence: 85,
            bankTxn: combo[0],
            ledgerTxns: [ledgerTxn],
            groupedBank: combo
          });
          
          break;
        }
      }
      
      if (ledgerTxn.matched) break;
    }
  });
  console.log(`After one-to-many matching: ${matched.length}`);

  const unmatchedBank = bankTransactions.filter(t => !t.matched);
  const unmatchedLedger = ledgerTransactions.filter(t => !t.matched);

  console.log(`Final: ${matched.length} matched, ${unmatchedBank.length} unmatched bank, ${unmatchedLedger.length} unmatched ledger`);

  const totalBankDebit = bankTransactions.reduce((sum, t) => sum + t.debit, 0);
  const totalBankCredit = bankTransactions.reduce((sum, t) => sum + t.credit, 0);
  const totalLedgerDebit = ledgerTransactions.reduce((sum, t) => sum + t.debit, 0);
  const totalLedgerCredit = ledgerTransactions.reduce((sum, t) => sum + t.credit, 0);
  
  const matchedBankAmount = matched.reduce((sum, m) => sum + m.bankTxn.amount, 0);
  const unmatchedBankAmount = unmatchedBank.reduce((sum, t) => sum + t.amount, 0);
  const unmatchedLedgerAmount = unmatchedLedger.reduce((sum, t) => sum + t.amount, 0);

  const matchRate = ((matched.length / Math.max(bankTransactions.length, ledgerTransactions.length)) * 100).toFixed(1);

  // Generate summary - JSON SAFE
  let summary = 'BANK RECONCILIATION STATEMENT\\n\\n';
  summary += `Reconciliation Date: ${new Date().toLocaleDateString()}\\n`;
  summary += `Match Rate: ${matchRate}%\\n`;
  summary += `Matching Engine: AI-Powered Multi-Rule Engine\\n\\n`;
  
  summary += 'SUMMARY STATISTICS\\n\\n';
  summary += '| Metric | Bank Statement | General Ledger | Difference |\\n';
  summary += '|--------|----------------|----------------|------------|\\n';
  summary += `| Total Transactions | ${bankTransactions.length} | ${ledgerTransactions.length} | ${Math.abs(bankTransactions.length - ledgerTransactions.length)} |\\n`;
  summary += `| Total Debits | ${totalBankDebit.toFixed(2)} | ${totalLedgerDebit.toFixed(2)} | ${Math.abs(totalBankDebit - totalLedgerDebit).toFixed(2)} |\\n`;
  summary += `| Total Credits | ${totalBankCredit.toFixed(2)} | ${totalLedgerCredit.toFixed(2)} | ${Math.abs(totalBankCredit - totalLedgerCredit).toFixed(2)} |\\n`;
  summary += `| Matched Transactions | ${matched.length} | ${matched.length} | - |\\n`;
  summary += `| Matched Amount | ${matchedBankAmount.toFixed(2)} | ${matchedBankAmount.toFixed(2)} | - |\\n`;
  summary += `| Unmatched Transactions | ${unmatchedBank.length} | ${unmatchedLedger.length} | - |\\n`;
  summary += `| Unmatched Amount | ${unmatchedBankAmount.toFixed(2)} | ${unmatchedLedgerAmount.toFixed(2)} | - |\\n\\n`;

  if (matched.length > 0) {
    summary += `MATCHED TRANSACTIONS (${matched.length} matches)\\n\\n`;
    summary += '| # | Match Type | Confidence | Bank Row | Ledger Row | Date | Amount | Debit | Credit | Bank Desc | Ledger Desc |\\n';
    summary += '|---|------------|------------|----------|------------|------|--------|-------|--------|-----------|-------------|\\n';
    matched.forEach((m, i) => {
      const ledgerRows = m.ledgerTxns.map(l => l.rowNumber).join(', ');
      const bankDesc = escapeForJson(m.bankTxn.description.substring(0, 25));
      const ledgerDesc = escapeForJson(m.ledgerTxns.map(l => l.description.substring(0, 25)).join('; '));
      
      summary += `| ${i + 1} | ${m.matchType} | ${m.confidence}% | ${m.bankTxn.rowNumber} | ${ledgerRows} | ${m.bankTxn.date} | ${m.bankTxn.amount.toFixed(2)} | ${m.bankTxn.debit.toFixed(2)} | ${m.bankTxn.credit.toFixed(2)} | ${bankDesc} | ${ledgerDesc} |\\n`;
    });
    summary += '\\n';
  }

  if (unmatchedBank.length > 0) {
    summary += `UNMATCHED BANK TRANSACTIONS (${unmatchedBank.length} items)\\n\\n`;
    summary += 'These transactions appear in Bank Statement but NOT in General Ledger:\\n\\n';
    summary += '| # | Row | Date | Description | Reference | Debit | Credit | Amount | Type |\\n';
    summary += '|---|-----|------|-------------|-----------|-------|--------|--------|------|\\n';
    unmatchedBank.forEach((t, i) => {
      const desc = escapeForJson(t.description.substring(0, 40));
      const ref = escapeForJson(t.reference.substring(0, 15));
      summary += `| ${i + 1} | ${t.rowNumber} | ${t.date} | ${desc} | ${ref} | ${t.debit.toFixed(2)} | ${t.credit.toFixed(2)} | ${t.amount.toFixed(2)} | ${t.type} |\\n`;
    });
    summary += `\\nTotal Unmatched Bank Amount: ${unmatchedBankAmount.toFixed(2)}\\n\\n`;
    
    summary += 'POSSIBLE REASONS FOR UNMATCHED BANK TRANSACTIONS:\\n';
    summary += '- Bank charges or fees not recorded in ledger\\n';
    summary += '- Interest income not yet journalized\\n';
    summary += '- Automatic payments or direct debits\\n';
    summary += '- NSF checks\\n';
    summary += '- Timing differences\\n\\n';
  }

  if (unmatchedLedger.length > 0) {
    summary += `UNMATCHED LEDGER TRANSACTIONS (${unmatchedLedger.length} items)\\n\\n`;
    summary += 'These transactions appear in General Ledger but NOT in Bank Statement:\\n\\n';
    summary += '| # | Row | Date | Description | Reference | Debit | Credit | Amount | Type |\\n';
    summary += '|---|-----|------|-------------|-----------|-------|--------|--------|------|\\n';
    unmatchedLedger.forEach((t, i) => {
      const desc = escapeForJson(t.description.substring(0, 40));
      const ref = escapeForJson(t.reference.substring(0, 15));
      summary += `| ${i + 1} | ${t.rowNumber} | ${t.date} | ${desc} | ${ref} | ${t.debit.toFixed(2)} | ${t.credit.toFixed(2)} | ${t.amount.toFixed(2)} | ${t.type} |\\n`;
    });
    summary += `\\nTotal Unmatched Ledger Amount: ${unmatchedLedgerAmount.toFixed(2)}\\n\\n`;
    
    summary += 'POSSIBLE REASONS FOR UNMATCHED LEDGER TRANSACTIONS:\\n';
    summary += '- Outstanding checks not yet cleared by bank\\n';
    summary += '- Deposits in transit\\n';
    summary += '- Post-dated checks\\n';
    summary += '- Timing differences\\n';
    summary += '- Electronic transfers in process\\n\\n';
  }

  summary += 'FORMAL BANK RECONCILIATION STATEMENT\\n\\n';
  const depositsInTransit = unmatchedLedger.filter(t => t.type === 'Credit').reduce((s, t) => s + t.amount, 0);
  const outstandingChecks = unmatchedLedger.filter(t => t.type === 'Debit').reduce((s, t) => s + t.amount, 0);
  const bankCollections = unmatchedBank.filter(t => t.type === 'Credit').reduce((s, t) => s + t.amount, 0);
  const bankCharges = unmatchedBank.filter(t => t.type === 'Debit').reduce((s, t) => s + t.amount, 0);
  
  summary += `Balance per Bank Statement: ${totalBankCredit.toFixed(2)}\\n`;
  summary += `Add: Deposits in Transit: ${depositsInTransit.toFixed(2)}\\n`;
  summary += `Less: Outstanding Checks: (${outstandingChecks.toFixed(2)})\\n`;
  summary += `Adjusted Bank Balance: ${(totalBankCredit + depositsInTransit - outstandingChecks).toFixed(2)}\\n\\n`;
  
  summary += `Balance per Books: ${totalLedgerCredit.toFixed(2)}\\n`;
  summary += `Add: Bank Collections: ${bankCollections.toFixed(2)}\\n`;
  summary += `Less: Bank Charges: (${bankCharges.toFixed(2)})\\n`;
  summary += `Adjusted Book Balance: ${(totalLedgerCredit + bankCollections - bankCharges).toFixed(2)}\\n\\n`;

  summary += 'ACTION ITEMS AND RECOMMENDATIONS\\n\\n';
  let actionNumber = 1;
  
  if (unmatchedBank.length > 0) {
    summary += `${actionNumber}. RECORD BANK TRANSACTIONS IN BOOKS\\n`;
    summary += `   ${unmatchedBank.length} bank transactions need journal entries\\n`;
    actionNumber++;
  }
  
  if (unmatchedLedger.length > 0) {
    summary += `${actionNumber}. INVESTIGATE OUTSTANDING ITEMS\\n`;
    summary += `   ${unmatchedLedger.length} ledger transactions not yet in bank\\n`;
    actionNumber++;
  }
  
  if (matchRate < 85) {
    summary += `${actionNumber}. IMPROVE DATA QUALITY\\n`;
    summary += `   Match rate of ${matchRate}% is below industry standard (over 90%)\\n`;
  }
  
  if (matchRate >= 95) {
    summary += `${actionNumber}. EXCELLENT RECONCILIATION\\n`;
    summary += `   Your records are well-maintained with ${matchRate}% match rate\\n`;
  }

  return {
    reconciled: true,
    summary,
    stats: {
      matchRate: parseFloat(matchRate),
      totalBankTransactions: bankTransactions.length,
      totalLedgerTransactions: ledgerTransactions.length,
      matchedCount: matched.length,
      unmatchedBankCount: unmatchedBank.length,
      unmatchedLedgerCount: unmatchedLedger.length,
      totalBankDebit: Number(totalBankDebit.toFixed(2)),
      totalBankCredit: Number(totalBankCredit.toFixed(2)),
      totalLedgerDebit: Number(totalLedgerDebit.toFixed(2)),
      totalLedgerCredit: Number(totalLedgerCredit.toFixed(2)),
      matchedAmount: Number(matchedBankAmount.toFixed(2)),
      unmatchedBankAmount: Number(unmatchedBankAmount.toFixed(2)),
      unmatchedLedgerAmount: Number(unmatchedLedgerAmount.toFixed(2))
    },
    matched,
    unmatchedBank,
    unmatchedLedger
  };
}

/**
 * GL Data preprocessing - UNCHANGED FROM ORIGINAL
 */
function preprocessGLDataFromRows(rows) {
  if (!rows || rows.length === 0) return { processed: false, reason: 'No rows' };

  const headers = Object.keys(rows[0]);

  const findColumn = (possibleNames) => {
    for (const name of possibleNames) {
      const found = headers.find(h => h.toLowerCase().includes(name.toLowerCase()));
      if (found) return found;
    }
    return null;
  };

  const accountCol = findColumn(['account', 'acc', 'gl account', 'account name', 'ledger', 'account desc']);
  const debitCol = findColumn(['debit', 'dr', 'debit amount', 'dr amount']);
  const creditCol = findColumn(['credit', 'cr', 'credit amount', 'cr amount']);
  const dateCol = findColumn(['date', 'trans date', 'transaction date', 'posting date', 'entry date']);
  const referenceCol = findColumn(['reference', 'ref', 'entry', 'journal', 'voucher', 'transaction']);
  const balanceCol = findColumn(['balance', 'net', 'amount']);

  if (!accountCol || (!debitCol && !creditCol && !balanceCol)) {
    return { processed: false, reason: 'Could not identify required columns', headers };
  }

  const accountSummary = {};
  let totalDebits = 0;
  let totalCredits = 0;
  let skippedRows = 0;
  let processedRows = 0;
  let minDate = null;
  let maxDate = null;
  let reversalEntries = 0;

  let debugInfo = [];

  rows.forEach((row, idx) => {
    const account = (row[accountCol] || '').toString().trim();
    if (!account) {
      skippedRows++;
      return;
    }

    const debitStr = debitCol ? (row[debitCol] || '').toString().trim() : '';
    const creditStr = creditCol ? (row[creditCol] || '').toString().trim() : '';

    let debit = 0;
    let credit = 0;

    const parsedDebit = parseAmount(debitStr || '');
    const parsedCredit = parseAmount(creditStr || '');

    if (parsedDebit !== 0 || parsedCredit !== 0) {
      if (parsedDebit < 0) {
        credit = Math.abs(parsedDebit);
        reversalEntries++;
      } else {
        debit = parsedDebit;
      }

      if (parsedCredit < 0) {
        debit = debit + Math.abs(parsedCredit);
        reversalEntries++;
      } else {
        credit = credit + parsedCredit;
      }
    } else {
      const amountColCandidate = balanceCol || (headers.find(h => /amount|amt|value/i.test(h)) || null);
      if (amountColCandidate && row[amountColCandidate] !== undefined) {
        const amt = parseAmount(row[amountColCandidate]);
        if (amt < 0) {
          credit = Math.abs(amt);
          reversalEntries++;
        } else {
          debit = amt;
        }
      }
    }

    if (dateCol && row[dateCol]) {
      const dateStr = row[dateCol].toString().trim();
      if (!minDate || dateStr < minDate) minDate = dateStr;
      if (!maxDate || dateStr > maxDate) maxDate = dateStr;
    }

    if (!accountSummary[account]) {
      accountSummary[account] = { account, totalDebit: 0, totalCredit: 0, count: 0 };
    }

    accountSummary[account].totalDebit += debit;
    accountSummary[account].totalCredit += credit;
    accountSummary[account].count += 1;

    if ((parsedDebit === 0 && parsedCredit === 0) && (debitStr || creditStr)) {
      debugInfo.push({ row: idx + 1, debitStr, creditStr, amountCandidate: row[balanceCol] });
    }

    totalDebits += debit;
    totalCredits += credit;
    processedRows++;
  });

  const accounts = Object.values(accountSummary).map(acc => ({
    account: acc.account,
    totalDebit: acc.totalDebit,
    totalCredit: acc.totalCredit,
    netBalance: acc.totalDebit - acc.totalCredit,
    totalActivity: acc.totalDebit + acc.totalCredit,
    count: acc.count
  })).sort((a,b) => b.totalActivity - a.totalActivity);

  const roundedDebits = Number(totalDebits.toFixed(2));
  const roundedCredits = Number(totalCredits.toFixed(2));
  const isBalanced = Math.abs(roundedDebits - roundedCredits) < 0.01;
  const difference = roundedDebits - roundedCredits;

  const formattedMinDate = formatDateUS(minDate);
  const formattedMaxDate = formatDateUS(maxDate);

  let summary = `## Pre-Processed GL Summary\\n\\n`;
  summary += `**Data Quality:**\\n`;
  summary += `- Total Rows: ${rows.length}\\n`;
  summary += `- Processed: ${processedRows} entries\\n`;
  summary += `- Skipped: ${skippedRows} entries\\n`;
  if (reversalEntries > 0) summary += `- Reversal Entries: ${reversalEntries} (negative amounts auto-corrected)\\n`;
  summary += `- Unique Accounts: ${accounts.length}\\n\\n`;
  if (formattedMinDate && formattedMaxDate) summary += `**Period:** ${formattedMinDate} to ${formattedMaxDate}\\n\\n`;

  summary += `**Financial Summary:**\\n`;
  summary += `- Total Debits: ${Math.round(roundedDebits).toLocaleString('en-US')}\\n`;
  summary += `- Total Credits: ${Math.round(roundedCredits).toLocaleString('en-US')}\\n`;
  summary += `- Difference: ${Math.round(difference).toLocaleString('en-US')}\\n`;
  summary += `- **Balanced:** ${isBalanced ? 'YES' : 'NO'}\\n\\n`;
  if (!isBalanced) summary += `WARNING: Debits and Credits do not balance. Difference of ${Math.round(Math.abs(difference)).toLocaleString('en-US')}\\n\\n`;

  summary += `### Account-wise Summary (All ${accounts.length} Accounts)\\n\\n`;
  summary += `| # | Account Name | Total Debit ($) | Total Credit ($) | Net Balance ($) | Entries |\\n`;
  summary += `|---|--------------|-----------------|------------------|-----------------|----------|\\n`;
  accounts.forEach((acc,i) => {
    summary += `| ${i+1} | ${acc.account} | ${Math.round(acc.totalDebit).toLocaleString('en-US')} | ${Math.round(acc.totalCredit).toLocaleString('en-US')} | ${Math.round(acc.netBalance).toLocaleString('en-US')} | ${acc.count} |\\n`;
  });

  return {
    processed: true,
    summary,
    stats: {
      totalDebits: roundedDebits,
      totalCredits: roundedCredits,
      difference,
      isBalanced,
      accountCount: accounts.length,
      entryCount: rows.length,
      processedCount: processedRows,
      skippedCount: skippedRows,
      reversalCount: reversalEntries,
      dateRange: formattedMinDate && formattedMaxDate ? `${formattedMinDate} to ${formattedMaxDate}` : 'Unknown'
    },
    accounts,
    debug: { sampleUnparsed: debugInfo.slice(0,10) }
  };
}

/**
 * PRE-PROCESS GL DATA - UNCHANGED
 */
function preprocessGLData(textOrRows) {
  if (Array.isArray(textOrRows)) {
    return preprocessGLDataFromRows(textOrRows);
  }

  const rows = parseCSV(textOrRows);
  return preprocessGLDataFromRows(rows);
}

/**
 * Detect document category - UNCHANGED
 */
function detectDocumentCategory(textContent) {
  const lower = textContent.toLowerCase();

  const glScore = (lower.match(/debit|credit|journal|gl entry/g) || []).length;
  const plScore = (lower.match(/revenue|profit|loss|income|expenses|ebitda/g) || []).length;

  console.log(`Category scores - GL: ${glScore}, P&L: ${plScore}`);

  if (glScore > plScore && glScore > 3) return 'gl';
  if (plScore > glScore && plScore > 3) return 'pl';

  return 'general';
}

/**
 * Get system prompt - UPDATED WITH BANK REC
 */
function getSystemPrompt(category, isPreprocessed = false, accountCount = 0) {
  if (category === 'bank_reconciliation') {
    return `You are an expert accounting assistant specialized in bank reconciliation.

The bank reconciliation has been performed automatically using a professional multi-rule matching engine. Your role is to:

1. Explain the reconciliation results clearly to the user
2. Highlight key findings matched vs unmatched transactions
3. Provide actionable insights on discrepancies
4. Suggest corrective actions for unmatched items
5. Explain timing differences checks in transit outstanding deposits

Focus Areas:
- Outstanding checks that have not cleared
- Deposits in transit
- Bank charges not recorded in books
- Errors in recording amounts or dates
- NSF checks
- Interest income or bank charges

Respond in clear professional markdown format with specific recommendations for each unmatched item.`;
  }

  if (category === 'gl') {
    return `You are an expert accounting assistant analyzing General Ledger entries.

INSTRUCTIONS:
1. You have access to the FULL COMPLETE General Ledger data analyze ALL entries in detail
2. DO NOT summarize examine every transaction every account every entry
3. If multiple sheets are present compare them thoroughly
4. Identify ALL unmatched items discrepancies missing entries or reconciliation issues
5. For bank reconciliation Match each bank transaction with corresponding GL entries
6. Highlight any transactions that appear in one sheet but not the other
7. Calculate totals but also show individual problematic transactions

Your Response Should Include:
1. Overview of all sheets data sources
2. Complete reconciliation analysis if applicable
3. List of ALL unmatched problematic items with transaction details
4. Account by account analysis where relevant
5. Specific recommendations for each issue found

Respond in clean markdown format with detailed tables showing problematic transactions.`;
  }

  if (category === 'pl') {
    return `You are an expert accounting assistant analyzing Profit and Loss statements.

Analyze the complete data and provide insights with observations and recommendations in markdown format.`;
  }

  return `You are an expert accounting assistant analyzing financial statements.

When totals exist USE those numbers. Create a markdown table with metrics and insights.`;
}

/**
 * Convert markdown to Word document - UNCHANGED
 */
async function markdownToWord(markdownText) {
  const sections = [];
  const lines = markdownText.split('\n');
  let tableData = [];
  let inTable = false;
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    
    if (!line) {
      if (sections.length > 0) {
        sections.push(new Paragraph({ text: '' }));
      }
      continue;
    }
    
    if (line.startsWith('#')) {
      const level = (line.match(/^#+/) || [''])[0].length;
      const text = line.replace(/^#+\s*/, '').replace(/\*\*/g, '').replace(/\*/g, '');
      
      sections.push(
        new Paragraph({
          text: text,
          heading: level === 2 ? HeadingLevel.HEADING_1 : HeadingLevel.HEADING_2,
          spacing: { before: 240, after: 120 },
          thematicBreak: false
        })
      );
      continue;
    }
    
    if (line.includes('|')) {
      const cells = line.split('|').map(c => c.trim()).filter(c => c !== '');
      
      if (cells.every(c => /^[-:]+$/.test(c))) {
        inTable = true;
        continue;
      }
      
      const cleanCells = cells.map(c => c.replace(/\*\*/g, '').replace(/\*/g, '').replace(/`/g, ''));
      tableData.push(cleanCells);
      continue;
    } else if (inTable && tableData.length > 0) {
      const tableRows = tableData.map((rowData, rowIdx) => {
        const isHeader = rowIdx === 0;
        
        return new TableRow({
          children: rowData.map(cellText => 
            new TableCell({
              children: [
                new Paragraph({
                  children: [
                    new TextRun({
                      text: cellText,
                      bold: isHeader,
                      color: isHeader ? 'FFFFFF' : '000000',
                      size: 22
                    })
                  ],
                  alignment: AlignmentType.LEFT
                })
              ],
              shading: {
                fill: isHeader ? '4472C4' : 'FFFFFF'
              },
              margins: {
                top: 100,
                bottom: 100,
                left: 100,
                right: 100
              }
            })
          )
        });
      });
      
      const table = new Table({
        rows: tableRows,
        width: {
          size: 100,
          type: WidthType.PERCENTAGE
        },
        borders: {
          top: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
          bottom: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
          left: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
          right: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
          insideHorizontal: { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' },
          insideVertical: { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' }
        }
      });
      
      sections.push(table);
      sections.push(new Paragraph({ text: '' }));
      tableData = [];
      inTable = false;
    }
    
    if (line.startsWith('-') || line.startsWith('*')) {
      let text = line.replace(/^[-*]\s+/, '');
      
      const textRuns = [];
      const parts = text.split(/(\*\*[^*]+\*\*)/g);
      
      parts.forEach(part => {
        if (part.startsWith('**') && part.endsWith('**')) {
          textRuns.push(new TextRun({
            text: part.replace(/\*\*/g, ''),
            bold: true
          }));
        } else if (part) {
          textRuns.push(new TextRun({ text: part }));
        }
      });
      
      sections.push(
        new Paragraph({
          children: textRuns,
          bullet: { level: 0 },
          spacing: { before: 60, after: 60 }
        })
      );
      continue;
    }
    
    const textRuns = [];
    const parts = line.split(/(\*\*[^*]+\*\*)/g);
    
    parts.forEach(part => {
      if (part.startsWith('**') && part.endsWith('**')) {
        textRuns.push(new TextRun({
          text: part.replace(/\*\*/g, ''),
          bold: true
        }));
      } else if (part) {
        textRuns.push(new TextRun({ text: part }));
      }
    });
    
    if (textRuns.length > 0) {
      sections.push(
        new Paragraph({
          children: textRuns,
          spacing: { before: 60, after: 60 }
        })
      );
    }
  }
  
  const doc = new Document({
    sections: [{
      properties: {},
      children: sections
    }]
  });
  
  const buffer = await Packer.toBuffer(doc);
  return buffer.toString('base64');
}

/**
 * Model call - UNCHANGED
 */
async function callModel({ fileType, textContent, question, category, preprocessedData, fullData }) {
  let content = textContent;
  
  if (category === 'bank_reconciliation' && preprocessedData) {
    content = preprocessedData.summary;
    console.log("Using bank reconciliation summary for AI analysis");
  } else if (category === 'gl' && fullData) {
    content = fullData;
    console.log("Using FULL GL data for detailed analysis");
  }

  const trimmed = content.length > 100000 
    ? content.slice(0, 100000) + "\\n\\n[Content truncated due to length]"
    : content;

  const systemPrompt = getSystemPrompt(category, false, 0);

  const messages = [
    { role: "system", content: systemPrompt },
    { 
      role: "user", 
      content: `File type: ${fileType}\\nDocument type: ${category.toUpperCase()}\\n\\nData contains ${content.length} characters.\\n\\n${trimmed}`
    },
    {
      role: "user",
      content: question || "Analyze this data in complete detail. If there are multiple sheets perform reconciliation and identify ALL unmatched items."
    }
  ];

  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`
    },
    body: JSON.stringify({
      model: process.env.OPENROUTER_MODEL || "tngtech/deepseek-r1t2-chimera:free",
      messages,
      temperature: 0.2,
      max_tokens: 4000
    })
  });

  let data;
  try {
    data = await r.json();
  } catch (err) {
    const raw = await r.text().catch(() => "");
    console.error("Model returned non-JSON:", raw.slice(0, 1000));
    return { reply: null, raw: { rawText: raw.slice(0, 2000), parseError: err.message }, httpStatus: r.status };
  }

  const reply = data?.choices?.[0]?.message?.content || data?.reply || null;

  return { reply, raw: data, httpStatus: r.status };
}

/**
 * MAIN handler - UPDATED WITH BANK REC DETECTION
 */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    if (!process.env.OPENROUTER_API_KEY) {
      return res.status(500).json({ error: "Missing OPENROUTER_API_KEY" });
    }

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body || {};
    const exportExcel = body.exportExcel !== undefined ? body.exportExcel : true;

    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });

    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType, buffer);

    let extracted = { type: detectedType, textContent: "" };
    
    if (detectedType === "pdf") {
      extracted = await extractPdf(buffer);
    } else if (detectedType === "docx") {
      extracted = await extractDocx(buffer);
    } else if (detectedType === "pptx") {
      extracted = await extractPptx(buffer);
    } else if (detectedType === "xlsx") {
      extracted = extractXlsx(buffer);
    } else if (["png", "jpg", "jpeg", "gif", "bmp", "webp"].includes(detectedType)) {
      extracted = await extractImage(buffer, detectedType);
    } else {
      extracted = extractCsv(buffer);
    }

    if (extracted.error) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: `Failed to parse file: ${extracted.error}`,
        debug: { error: extracted.error }
      });
    }

    if (extracted.ocrNeeded) {
      return res.status(200).json({
        ok: false,
        type: "pdf",
        reply: "This PDF appears to be scanned image-based and requires OCR. Please upload the scanned document as an image file PNG JPG instead our OCR system works better with direct image files than scanned PDFs.",
        debug: { ocrNeeded: true, error: extracted.error }
      });
    }
    
    if (extracted.requiresVision || extracted.requiresManualProcessing || extracted.requiresConversion) {
      return res.status(200).json({
        ok: true,
        type: extracted.type,
        reply: extracted.textContent || "This file type requires conversion. Please see the instructions below.",
        category: "general",
        preprocessed: false,
        debug: { 
          requiresConversion: extracted.requiresConversion || false,
          requiresManualProcessing: extracted.requiresManualProcessing || false,
          isImage: extracted.isImage || false,
          message: "File needs to be converted to a supported format"
        }
      });
    }

    let preprocessedData = null;
    let category = 'general';
    let fullDataForGL = null;
    
    // BANK RECONCILIATION DETECTION - NEW FEATURE
    if (extracted.rows) {
      const sheetNames = [...new Set(extracted.rows.map(r => r.__sheet_name))];
      
      const hasBankSheet = sheetNames.some(name => name && name.toLowerCase().includes('bank'));
      const hasLedgerSheet = sheetNames.some(name => name && (name.toLowerCase().includes('ledger') || name.toLowerCase().includes('gl')));
      
      if (hasBankSheet && hasLedgerSheet) {
        console.log("=== BANK RECONCILIATION DETECTED ===");
        category = 'bank_reconciliation';
        
        const reconciliationData = performBankReconciliation(extracted.rows);
        
        if (!reconciliationData.reconciled) {
          return res.status(200).json({
            ok: false,
            type: 'xlsx',
            reply: reconciliationData.error || 'Bank reconciliation failed',
            category: 'bank_reconciliation',
            debug: {
              reason: reconciliationData.reason,
              sheetsFound: reconciliationData.sheetsFound,
              bankHeaders: reconciliationData.bankHeaders,
              ledgerHeaders: reconciliationData.ledgerHeaders
            }
          });
        }
        
        preprocessedData = reconciliationData;
        fullDataForGL = reconciliationData.summary;
        
        console.log(`Bank Reconciliation Complete: ${reconciliationData.stats.matchRate}% match rate`);
        console.log(`Matched: ${reconciliationData.stats.matchedCount} Unmatched Bank: ${reconciliationData.stats.unmatchedBankCount} Unmatched Ledger: ${reconciliationData.stats.unmatchedLedgerCount}`);
      } else {
        // EXISTING GL LOGIC - UNCHANGED
        const sampleText = JSON.stringify(extracted.rows.slice(0, 20)).toLowerCase();
        category = detectDocumentCategory(sampleText);
        
        if (category === 'gl') {
          const headers = Object.keys(extracted.rows[0] || {}).filter(h => h !== '__sheet_name');
          const csvLines = [headers.join(',')];
          
          let currentSheet = null;
          extracted.rows.forEach(row => {
            if (row.__sheet_name && row.__sheet_name !== currentSheet) {
              currentSheet = row.__sheet_name;
              csvLines.push(`\\n### Sheet: ${currentSheet} ###`);
            }
            
            const values = headers.map(h => {
              const val = row[h] || '';
              return typeof val === 'string' && (val.includes(',') || val.includes('"')) 
                ? `"${val.replace(/"/g, '""')}"` 
                : val;
            });
            csvLines.push(values.join(','));
          });
          
          fullDataForGL = csvLines.join('\\n');
          console.log(`Prepared full GL data: ${fullDataForGL.length} characters ${extracted.rows.length} rows`);
          
          preprocessedData = preprocessGLData(extracted.rows);
          console.log("GL preprocessing result:", preprocessedData.processed ? "SUCCESS" : "FAILED");
        }
      }
    } else {
      // EXISTING TEXT PROCESSING - UNCHANGED
      const textContent = extracted.textContent || '';
      if (!textContent.trim()) {
        return res.status(200).json({
          ok: false,
          type: extracted.type,
          reply: "No text could be extracted from this file.",
          debug: { contentType, bytesReceived }
        });
      }

      category = detectDocumentCategory(textContent);
      console.log(`Category: ${category}`);

      if (category === 'gl') {
        fullDataForGL = textContent;
        preprocessedData = preprocessGLData(textContent);
        console.log("GL preprocessing result:", preprocessedData.processed ? "SUCCESS" : "FAILED");
      }
    }

    const { reply, raw, httpStatus } = await callModel({
      fileType: extracted.type,
      textContent: extracted.textContent || '',
      question,
      category,
      preprocessedData,
      fullData: fullDataForGL
    });

    if (!reply) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: "(No reply from model)",
        debug: { status: httpStatus, raw: raw }
      });
    }

    // WORD DOCUMENT GENERATION - UNCHANGED
    let wordBase64 = null;
    try {
      console.log("Starting Word document generation...");
      wordBase64 = await markdownToWord(reply);
      console.log("Word document generated successfully length:", wordBase64.length);
    } catch (wordError) {
      console.error("Word generation error:", wordError);
    }

    return res.status(200).json({
      ok: true,
      type: extracted.type,
      category,
      reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      wordSize: wordBase64 ? wordBase64.length : 0,
      preprocessed: preprocessedData?.processed || preprocessedData?.reconciled || false,
      debug: {
        status: httpStatus,
        category,
        preprocessed: preprocessedData?.processed || preprocessedData?.reconciled || false,
        stats: preprocessedData?.stats || null,
        debug_sample: preprocessedData?.debug || null,
        hasWord: !!wordBase64,
        wordGenerated: !!wordBase64
      }
    });
  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ 
      error: String(err?.message || err)
    });
  }
}
  import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";
import { Document, Paragraph, TextRun, Table, TableRow, TableCell, WidthType, BorderStyle, AlignmentType, HeadingLevel, Packer } from "docx";
import JSZip from "jszip";

/**
 * CORS helper
 */
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

/**
 * Tolerant body parser
 */
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!body) return resolve({});
      const contentType =
        (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "";
      if (contentType.includes("application/json")) {
        try {
          const parsed = JSON.parse(body);
          return resolve(parsed);
        } catch (err) {
          return resolve({ userMessage: body });
        }
      }
      try {
        const parsed = JSON.parse(body);
        return resolve(parsed);
      } catch {
        return resolve({ userMessage: body });
      }
    });
    req.on("error", reject);
  });
}

/**
 * Download remote file into Buffer
 */
async function downloadFileToBuffer(
  url,
  maxBytes = 30 * 1024 * 1024,
  timeoutMs = 20000
) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  let r;
  try {
    r = await fetch(url, { signal: controller.signal });
  } catch (err) {
    clearTimeout(timer);
    throw new Error(`Download failed or timed out: ${err.message || err}`);
  }
  clearTimeout(timer);

  if (!r.ok) throw new Error(`Failed to download file: ${r.status} ${r.statusText}`);

  const contentType = r.headers.get("content-type") || "";
  const chunks = [];
  let total = 0;

  try {
    for await (const chunk of r.body) {
      total += chunk.length;
      if (total > maxBytes) {
        const allowed = maxBytes - (total - chunk.length);
        if (allowed > 0) chunks.push(chunk.slice(0, allowed));
        break;
      } else {
        chunks.push(chunk);
      }
    }
  } catch (err) {
    throw new Error(`Error reading download stream: ${err.message || err}`);
  }

  console.log(`Downloaded ${total} bytes, content-type: ${contentType}`);
  return { buffer: Buffer.concat(chunks), contentType, bytesReceived: total };
}

/**
 * Detect file type
 */
function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();

  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      if (lowerUrl.includes('.docx') || lowerType.includes('wordprocessing')) return "docx";
      if (lowerUrl.includes('.pptx') || lowerType.includes('presentation')) return "pptx";
      return "xlsx";
    }
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46)
      return "pdf";
    if (buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4E && buffer[3] === 0x47)
      return "png";
    if (buffer[0] === 0xFF && buffer[1] === 0xD8 && buffer[2] === 0xFF)
      return "jpg";
    if (buffer[0] === 0x47 && buffer[1] === 0x49 && buffer[2] === 0x46)
      return "gif";
  }

  if (lowerUrl.endsWith(".pdf") || lowerType.includes("application/pdf")) return "pdf";
  if (lowerUrl.endsWith(".docx") || lowerType.includes("wordprocessing")) return "docx";
  if (lowerUrl.endsWith(".doc")) return "doc";
  if (lowerUrl.endsWith(".pptx") || lowerType.includes("presentation")) return "pptx";
  if (lowerUrl.endsWith(".ppt")) return "ppt";
  if (
    lowerUrl.endsWith(".xlsx") ||
    lowerUrl.endsWith(".xls") ||
    lowerType.includes("spreadsheet") ||
    lowerType.includes("sheet") ||
    lowerType.includes("excel")
  ) return "xlsx";
  if (lowerUrl.endsWith(".csv") || lowerType.includes("text/csv")) return "csv";
  if (lowerUrl.endsWith(".png") || lowerType.includes("image/png")) return "png";
  if (lowerUrl.endsWith(".jpg") || lowerUrl.endsWith(".jpeg") || lowerType.includes("image/jpeg")) return "jpg";
  if (lowerUrl.endsWith(".gif") || lowerType.includes("image/gif")) return "gif";
  if (lowerUrl.endsWith(".bmp") || lowerType.includes("image/bmp")) return "bmp";
  if (lowerUrl.endsWith(".webp") || lowerType.includes("image/webp")) return "webp";

  return "csv";
}

/**
 * Convert buffer to UTF-8 text
 */
function bufferToText(buffer) {
  if (!buffer) return "";
  let text = buffer.toString("utf8");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  return text;
}

/**
 * Extract CSV
 */
function extractCsv(buffer) {
  const text = bufferToText(buffer);
  return { type: "csv", textContent: text };
}

/**
 * Robust numeric parser for accounting amounts
 */
function parseAmount(s) {
  if (s === null || s === undefined) return 0;
  let str = String(s).trim();

  if (!str) return 0;

  const parenMatch = str.match(/^\s*\((.*)\)\s*$/);
  if (parenMatch) str = '-' + parenMatch[1];

  const trailingMinus = str.match(/^(.*?)[\s-]+$/);
  if (trailingMinus && !/^-/.test(str)) {
    str = '-' + trailingMinus[1];
  }

  const crMatch = str.match(/\bCR\b/i);
  const drMatch = str.match(/\bDR\b/i);
  if (crMatch && !drMatch) {
    if (!str.includes('-')) str = '-' + str;
  } else if (drMatch && !crMatch) {
    str = str.replace('-', '');
  }

  str = str.replace(/[^0-9.\-]/g, '');
  const parts = str.split('.');
  if (parts.length > 2) {
    str = parts.shift() + '.' + parts.join('');
  }

  const n = parseFloat(str);
  if (Number.isNaN(n)) return 0;
  return n;
}

/**
 * Format date to US format (MM/DD/YYYY)
 */
function formatDateUS(dateStr) {
  if (!dateStr) return dateStr;
  
  const num = parseFloat(dateStr);
  if (!isNaN(num) && num > 40000 && num < 50000) {
    const date = new Date((num - 25569) * 86400 * 1000);
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const year = date.getFullYear();
    return `${month}/${day}/${year}`;
  }
  
  const date = new Date(dateStr);
  if (!isNaN(date.getTime())) {
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const year = date.getFullYear();
    return `${month}/${day}/${year}`;
  }
  
  return dateStr;
}

/**
 * Escape special characters for JSON
 */
function escapeForJson(str) {
  if (!str) return '';
  return String(str)
    .replace(/\\/g, '\\\\')
    .replace(/"/g, '\\"')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r')
    .replace(/\t/g, '\\t');
}

/**
 * Fuzzy string similarity
 */
function stringSimilarity(str1, str2) {
  const s1 = str1.toLowerCase().trim();
  const s2 = str2.toLowerCase().trim();
  
  if (s1 === s2) return 1.0;
  if (s1.length === 0 || s2.length === 0) return 0.0;
  if (s1.includes(s2) || s2.includes(s1)) return 0.8;
  
  const matrix = [];
  for (let i = 0; i <= s2.length; i++) {
    matrix[i] = [i];
  }
  for (let j = 0; j <= s1.length; j++) {
    matrix[0][j] = j;
  }
  for (let i = 1; i <= s2.length; i++) {
    for (let j = 1; j <= s1.length; j++) {
      if (s2.charAt(i - 1) === s1.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          matrix[i][j - 1] + 1,
          matrix[i - 1][j] + 1
        );
      }
    }
  }
  
  const distance = matrix[s2.length][s1.length];
  const maxLen = Math.max(s1.length, s2.length);
  return 1 - (distance / maxLen);
}

/**
 * Get combinations
 */
function getCombinations(arr, size) {
  if (size > arr.length || size <= 0) return [];
  if (size === arr.length) return [arr];
  if (size === 1) return arr.map(el => [el]);
  
  const combinations = [];
  
  function combine(start, chosen) {
    if (chosen.length === size) {
      combinations.push([...chosen]);
      return;
    }
    
    for (let i = start; i < arr.length; i++) {
      chosen.push(arr[i]);
      combine(i + 1, chosen);
      chosen.pop();
    }
  }
  
  combine(0, []);
  return combinations;
}

/**
 * Extract XLSX - READ ALL COLUMNS
 */
function extractXlsx(buffer) {
  try {
    console.log("Starting XLSX extraction...");
    const workbook = XLSX.read(buffer, {
      type: "buffer",
      cellDates: false,
      cellNF: false,
      cellText: true,
      raw: false,
      defval: ''
    });

    console.log(`XLSX has ${workbook.SheetNames.length} sheets:`, workbook.SheetNames);

    if (workbook.SheetNames.length === 0) {
      console.log("No sheets found");
      return { type: "xlsx", textContent: "", rows: [] };
    }

    let allRows = [];
    let allCsv = '';

    workbook.SheetNames.forEach((sheetName, index) => {
      console.log(`Processing sheet ${index + 1}/${workbook.SheetNames.length}: "${sheetName}"`);
      
      const sheet = workbook.Sheets[sheetName];
      
      const jsonRows = XLSX.utils.sheet_to_json(sheet, { defval: '', blankrows: true, raw: false });
      console.log(`  - Sheet "${sheetName}" has ${jsonRows.length} rows`);
      
      const rowsWithSheetName = jsonRows.map((row, idx) => ({
        ...row,
        __sheet_name: sheetName,
        __row_number: idx + 2
      }));
      
      allRows = allRows.concat(rowsWithSheetName);
      
      const csv = XLSX.utils.sheet_to_csv(sheet, {
        blankrows: true,
        FS: ',',
        RS: '\n',
        strip: false,
        rawNumbers: false
      });
      
      if (index > 0) allCsv += '\n\n';
      allCsv += `Sheet: ${sheetName}\n${csv}`;
    });

    console.log(`Total rows from all sheets: ${allRows.length}`);

    return { type: "xlsx", textContent: allCsv, rows: allRows, sheetCount: workbook.SheetNames.length };
  } catch (err) {
    console.error("extractXlsx failed:", err?.message || err);
    return { type: "xlsx", textContent: "", rows: [], error: String(err?.message || err) };
  }
}

/**
 * Extract Word Document (.docx)
 */
async function extractDocx(buffer) {
  console.log("=== DOCX EXTRACTION with JSZip ===");
  
  try {
    const zip = await JSZip.loadAsync(buffer);
    console.log("ZIP loaded, files:", Object.keys(zip.files).join(', '));
    
    const documentXml = zip.files['word/document.xml'];
    
    if (!documentXml) {
      console.log("document.xml not found");
      return { 
        type: "docx", 
        textContent: "", 
        error: "Invalid Word document structure" 
      };
    }
    
    const xmlContent = await documentXml.async('text');
    console.log("XML content length:", xmlContent.length);
    
    const textRegex = /<w:t[^>]*>([^<]+)<\/w:t>/g;
    const textParts = [];
    let match;
    
    while ((match = textRegex.exec(xmlContent)) !== null) {
      if (match[1]) {
        const text = match[1]
          .replace(/&lt;/g, '<')
          .replace(/&gt;/g, '>')
          .replace(/&amp;/g, '&')
          .replace(/&quot;/g, '"')
          .replace(/&apos;/g, "'")
          .trim();
        
        if (text.length > 0) {
          textParts.push(text);
        }
      }
    }
    
    console.log("Extracted text elements:", textParts.length);
    
    if (textParts.length === 0) {
      return { 
        type: "docx", 
        textContent: "", 
        error: "No text found in Word document. Document may be empty or contain only images." 
      };
    }
    
    const fullText = textParts.join(' ');
    console.log("Final text length:", fullText.length);
    
    return { 
      type: "docx", 
      textContent: fullText 
    };
    
  } catch (error) {
    console.error("DOCX extraction error:", error.message);
    return { 
      type: "docx", 
      textContent: "", 
      error: `Failed to read Word document: ${error.message}` 
    };
  }
}

/**
 * Extract PowerPoint (.pptx)
 */
async function extractPptx(buffer) {
  try {
    const bufferStr = buffer.toString('latin1');
    
    const textPattern = /<a:t[^>]*>([^<]+)<\/a:t>/g;
    let match;
    let allText = [];
    
    while ((match = textPattern.exec(bufferStr)) !== null) {
      const text = match[1];
      const cleaned = text
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&')
        .replace(/&quot;/g, '"')
        .replace(/&apos;/g, "'")
        .trim();
      
      if (cleaned && cleaned.length > 0) {
        allText.push(cleaned);
      }
    }
    
    if (allText.length < 5) {
      const paraPattern = /<a:p[^>]*>(.*?)<\/a:p>/gs;
      const paraMatches = bufferStr.matchAll(paraPattern);
      
      for (const match of paraMatches) {
        const innerText = match[1].replace(/<[^>]+>/g, ' ').trim();
        if (innerText.length > 2) {
          allText.push(innerText);
        }
      }
    }
    
    if (allText.length === 0) {
      return { 
        type: "pptx", 
        textContent: "", 
        error: "No text found in PowerPoint. Please try exporting as PDF." 
      };
    }
    
    const text = allText.join('\n').trim();
    
    console.log(`Extracted ${text.length} characters from PPTX`);
    
    if (text.length < 20) {
      return { 
        type: "pptx", 
        textContent: "", 
        error: "Presentation appears to be empty or contains mostly images" 
      };
    }
    
    return { type: "pptx", textContent: text };
  } catch (err) {
    console.error("extractPptx failed:", err?.message || err);
    return { 
      type: "pptx", 
      textContent: "", 
      error: String(err?.message || err) 
    };
  }
}

/**
 * Extract PDF
 */
async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = (data && data.text) ? data.text.trim() : "";

    if (!text || text.length < 50) {
      console.log("PDF appears to be scanned or image-based, attempting OCR...");
      
      return { 
        type: "pdf", 
        textContent: "", 
        ocrNeeded: true,
        error: "This PDF appears to be scanned (image-based). Please try uploading the original image files (PNG/JPG) instead, or use a PDF with selectable text."
      };
    }

    return { type: "pdf", textContent: text, ocrNeeded: false };
  } catch (err) {
    console.error("extractPdf failed:", err?.message || err);
    return { type: "pdf", textContent: "", error: String(err?.message || err) };
  }
}

/**
 * Extract Image
 */
async function extractImage(buffer, fileType) {
  try {
    console.log(`Image upload detected: ${fileType}, size: ${(buffer.length / 1024).toFixed(2)} KB`);
    
    const helpMessage = `Image File Detected (${fileType.toUpperCase()})

I can help you extract text from this image using these FREE methods:

FASTEST METHOD - Use Google Drive (100% Free):
1. Upload your image to Google Drive
2. Right-click and select Open with Google Docs
3. Google will automatically OCR the image and convert to editable text
4. Copy the text and paste it here OR
5. Download as PDF and upload that PDF to me

METHOD 2 - Use Your Phone:
Most phones have built-in scanners:
- iPhone: Notes app and select Scan Documents
- Android: Google Drive and select Scan
- These create searchable PDFs automatically

METHOD 3 - Free Online OCR Tools:
- onlineocr.net (no signup needed)
- i2ocr.com (simple and fast)
- newocr.com (supports 122 languages)

METHOD 4 - Convert to PDF:
If this is a scan convert it to a searchable PDF using:
- Adobe Acrobat (free trial)
- PDF24 Tools (free online)
- SmallPDF (3 free conversions per day)

Image Info:
- Type: ${fileType.toUpperCase()}
- Size: ${(buffer.length / 1024).toFixed(2)} KB
- Ready for OCR: Yes

Once you have the text or searchable PDF upload it here and I will analyze it immediately`;
    
    return { 
      type: fileType, 
      textContent: helpMessage,
      isImage: true,
      requiresManualProcessing: true
    };
    
  } catch (err) {
    console.error("Image handling error:", err?.message || err);
    return { 
      type: fileType, 
      textContent: "", 
      error: "Error processing image. Please convert to PDF or extract text manually."
    };
  }
}

/**
 * Parse CSV to array of objects
 */
function parseCSV(csvText) {
  const lines = csvText.trim().split('\n');
  if (lines.length < 2) return [];

  const parseCSVLine = (line) => {
    const result = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      const nextChar = line[i + 1];

      if (char === '"') {
        if (inQuotes && nextChar === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === ',' && !inQuotes) {
        result.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    result.push(current.trim());
    return result;
  };

  const headers = parseCSVLine(lines[0]);
  const headerCount = headers.length;
  const rows = [];

  console.log(`CSV has ${lines.length} lines total (including header)`);
  console.log(`Headers (${headerCount} columns):`, headers);

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];

    if (!line || line.trim() === '' || line.trim() === ','.repeat(headerCount - 1)) {
      continue;
    }

    const values = parseCSVLine(line);

    const row = {};
    headers.forEach((h, idx) => {
      row[h] = values[idx] !== undefined ? values[idx] : '';
    });

    rows.push(row);
  }

  console.log(`Parsed ${rows.length} data rows (should match Excel row count minus header)`);
  return rows;
}

/**
 * BANK RECONCILIATION ENGINE
 */
function performBankReconciliation(rows) {
  console.log("=== BANK RECONCILIATION ENGINE STARTED ===");
  
  if (!rows || rows.length === 0) {
    return { 
      reconciled: false, 
      error: 'No data found in Excel file'
    };
  }

  const bankSheet = rows.filter(r => r.__sheet_name && r.__sheet_name.toLowerCase().includes('bank'));
  const ledgerSheet = rows.filter(r => r.__sheet_name && (r.__sheet_name.toLowerCase().includes('ledger') || r.__sheet_name.toLowerCase().includes('gl')));
  
  console.log(`Bank sheet: ${bankSheet.length} rows`);
  console.log(`Ledger sheet: ${ledgerSheet.length} rows`);

  if (bankSheet.length === 0 || ledgerSheet.length === 0) {
    const availableSheets = [...new Set(rows.map(r => r.__sheet_name))];
    return {
      reconciled: false,
      error: `Missing required sheets. Found sheets: ${availableSheets.join(', ')}. Please ensure one sheet contains Bank and another contains Ledger or GL in the name.`
    };
  }

  const bankHeaders = Object.keys(bankSheet[0] || {}).filter(h => h !== '__sheet_name' && h !== '__row_number');
  const ledgerHeaders = Object.keys(ledgerSheet[0] || {}).filter(h => h !== '__sheet_name' && h !== '__row_number');
  
  console.log("Bank headers:", bankHeaders);
  console.log("Ledger headers:", ledgerHeaders);
  
  const findColumn = (headers, possibleNames) => {
    for (const name of possibleNames) {
      const found = headers.find(h => h.toLowerCase().includes(name.toLowerCase()));
      if (found) return found;
    }
    return null;
  };

  const bankDateCol = findColumn(bankHeaders, ['date', 'transaction date', 'trans date', 'posting date', 'value date']);
  const bankDescCol = findColumn(bankHeaders, ['description', 'desc', 'particulars', 'narration', 'details', 'memo']);
  const bankRefCol = findColumn(bankHeaders, ['reference', 'ref', 'cheque', 'check', 'transaction id', 'ref no', 'document', 'doc no']);
  const bankAmountCol = findColumn(bankHeaders, ['amount', 'transaction amount', 'value']);
  const bankDebitCol = findColumn(bankHeaders, ['debit', 'withdrawal', 'dr', 'debit amount']);
  const bankCreditCol = findColumn(bankHeaders, ['credit', 'deposit', 'cr', 'credit amount']);

  const ledgerDateCol = findColumn(ledgerHeaders, ['date', 'transaction date', 'trans date', 'posting date', 'entry date', 'value date']);
  const ledgerDescCol = findColumn(ledgerHeaders, ['description', 'desc', 'particulars', 'narration', 'details', 'memo']);
  const ledgerRefCol = findColumn(ledgerHeaders, ['reference', 'ref', 'voucher', 'journal', 'entry no', 'document', 'doc no']);
  const ledgerAmountCol = findColumn(ledgerHeaders, ['amount', 'value']);
  const ledgerDebitCol = findColumn(ledgerHeaders, ['debit', 'dr', 'debit amount']);
  const ledgerCreditCol = findColumn(ledgerHeaders, ['credit', 'cr', 'credit amount']);

  console.log("Detected Bank columns:", { bankDateCol, bankDescCol, bankRefCol, bankAmountCol, bankDebitCol, bankCreditCol });
  console.log("Detected Ledger columns:", { ledgerDateCol, ledgerDescCol, ledgerRefCol, ledgerAmountCol, ledgerDebitCol, ledgerCreditCol });

  if (!bankDateCol && !ledgerDateCol) {
    return {
      reconciled: false,
      error: 'Could not find date columns. Please ensure at least one sheet has a column with Date in the header.',
      bankHeaders,
      ledgerHeaders
    };
  }

  const bankTransactions = bankSheet.map((row, idx) => {
    const date = bankDateCol ? formatDateUS(row[bankDateCol]) : '';
    const description = bankDescCol ? String(row[bankDescCol] || '').trim() : '';
    const reference = bankRefCol ? String(row[bankRefCol] || '').trim() : '';
    
    let debit = 0;
    let credit = 0;
    let amount = 0;
    
    if (bankDebitCol && bankCreditCol) {
      debit = parseAmount(row[bankDebitCol] || '');
      credit = parseAmount(row[bankCreditCol] || '');
      amount = debit > 0 ? debit : credit;
    } else if (bankAmountCol) {
      const amt = parseAmount(row[bankAmountCol] || '');
      amount = Math.abs(amt);
      if (amt < 0) {
        credit = amount;
      } else {
        debit = amount;
      }
    }
    
    if (amount === 0) return null;
    
    return {
      id: `BANK-${idx + 1}`,
      rowNumber: row.__row_number || (idx + 2),
      date,
      description,
      reference,
      debit,
      credit,
      amount,
      type: debit > 0 ? 'Debit' : 'Credit',
      matched: false,
      matchedWith: [],
      matchType: null,
      matchScore: 0
    };
  }).filter(t => t !== null);

  const ledgerTransactions = ledgerSheet.map((row, idx) => {
    const date = ledgerDateCol ? formatDateUS(row[ledgerDateCol]) : '';
    const description = ledgerDescCol ? String(row[ledgerDescCol] || '').trim() : '';
    const reference = ledgerRefCol ? String(row[ledgerRefCol] || '').trim() : '';
    
    let debit = 0;
    let credit = 0;
    let amount = 0;
    
    if (ledgerDebitCol && ledgerCreditCol) {
      debit = parseAmount(row[ledgerDebitCol] || '');
      credit = parseAmount(row[ledgerCreditCol] || '');
      amount = debit > 0 ? debit : credit;
    } else if (ledgerAmountCol) {
      const amt = parseAmount(row[ledgerAmountCol] || '');
      amount = Math.abs(amt);
      if (amt < 0) {
        credit = amount;
      } else {
        debit = amount;
      }
    }
    
    if (amount === 0) return null;
    
    return {
      id: `LEDGER-${idx + 1}`,
      rowNumber: row.__row_number || (idx + 2),
      date,
      description,
      reference,
      debit,
      credit,
      amount,
      type: debit > 0 ? 'Debit' : 'Credit',
      matched: false,
      matchedWith: [],
      matchType: null,
      matchScore: 0
    };
  }).filter(t => t !== null);

  console.log(`Parsed ${bankTransactions.length} bank transactions`);
  console.log(`Parsed ${ledgerTransactions.length} ledger transactions`);

  if (bankTransactions.length === 0 || ledgerTransactions.length === 0) {
    return {
      reconciled: false,
      error: 'No valid transactions found in one or both sheets. Please check your data.'
    };
  }

  const matched = [];
  
  // RULE 1: EXACT MATCH
  console.log("Rule 1: Exact matching..
