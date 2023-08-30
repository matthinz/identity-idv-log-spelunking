report.md: report.template.md scripts/*.rb
	ruby scripts/examine-journeys.rb < report.template.md > $@

report.docx: report.md
	pandoc -i $< -o $@
