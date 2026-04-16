# Business Problem

Securitize is the transfer agent, fund administrator, and ATS operator for
tokenized funds totaling $3.7B+ AUM, partnering with asset managers including
Apollo, BlackRock, Hamilton Lane, and KKR. In this setup the company bears
regulatory obligations that depend entirely on data quality:

- **Transfer agent recordkeeping** (SEC 17Ad) requires an accurate, auditable
  investor register that reconciles to on-chain token holdings.
- **Fund administration** requires complete, plausible NAV strikes every
  business day and reconciled subscription / redemption activity.
- **Reg D 506(c) compliance** requires accredited investor verification for US
  holders.
- **ATS operation** requires trade reporting that ties to on-chain settlement.

Every one of those obligations breaks if the data doesn't reconcile. A cap
table that disagrees with the blockchain is not a bookkeeping annoyance — it
is a material audit exception.

The Data & AI Generalist role exists because this surface area is widening
faster than the team. New funds come online, new asset managers integrate,
new jurisdictions apply, and the data shape evolves with each one. You need
someone who can:

- Write the reconciliation queries
- Package them into a validation system with owners and severities
- Build the dashboards Compliance and Finance actually use
- Layer AI (anomaly detection, compliance automation, internal chatbots) on
  top of trustworthy data — not unreliable data

This prototype targets the first three. The AI layer is represented by the
anomaly detector; the chatbot / automation pieces slot in behind the same
validated data and finding ontology.
