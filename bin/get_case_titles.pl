#!/usr/bin/env perl

# Paul Gordon, 2020
# Script to read in Zotero bibliographic information (RDF) and create annotations for NextStrain that link individual virus names back to the most suitable (as defined by manual override or earliest date) publication that references them.

use strict;
use warnings;
use RDF::Simple::Parser;

@ARGV > 2 or die "Usage: $0 <sample2precedent.txt> <gisaid_patient_status_metadata.txt> file:zotero_bibliography_file1.rdf [...file2.rdf...]\n";

my $override_file = shift @ARGV;
my $gisaid_metadata_file = shift @ARGV;

open(OVERRIDE, $override_file)
  or die "Cannot open $override_file for reading: $!\n";
my %superceding_sample2url;
while(<OVERRIDE>){
	next if /^\s*#/;
	chomp;
	my @F = split /\t/, $_;
	if($#F != 1){
		warn "Skipping malformatted line ($override_file #$.)\n";
		next;
	}
	my ($sample) = $F[0];
	$sample =~ s/^\s+//; # remove any leading or trailing spaces
	$sample =~ s/\s+$//;
	if(exists $superceding_sample2url{$sample}){
		if($superceding_sample2url{$sample} eq $F[1]){
			warn "Skipping repeated superceding line ($override_file #$.)\n";
		}
		else{
			warn "Conflicting superceding values for sample '$sample', keeping '",$superceding_sample2url{$F[0]},"', and ignoring redefinition '$F[1]' ($override_file #$.)\n";
		}
                next;
	}
	$superceding_sample2url{$sample} = $F[1];
}
close(OVERRIDE);

# GISAID Patient Status metadata looks like below:
# We gratefully acknowledge the following Authors from the Originating laboratories responsible for obtaining the specimens, as well as the Submitting laboratories where the genome data were generated and shared via GISAID, on which this research is based.
# All Submitters of data may be contacted directly via www.gisaid.org
# Virus name      Accession ID    Collection date Location        Host    Additional location information Gender  Patient age     Patient status  Passage Specimen        Additional host information     Lineage Clade
# hCoV-19/Jusridiction/ID/2020  EPI_ISL_######  2020-01-22      ...
my %sample2gisaid_id;
my %duplicate_sample2gisaid_id;
open(GISAID, $gisaid_metadata_file)
  or die "Cannot open $gisaid_metadata_file for reading: $!\n";
while(<GISAID>){
	next if /^(?:GISAID|We grat|All Sub|Virus name)/; # headers, possible more than once if metadata exported in chunks and concatenated (Web portal limits to 10K entries per download)
	chomp;
	my @F = split /\t/, $_;
	my ($sample) = $F[0];
        $sample	=~ s/^hCoV-19\///;
	if(exists $sample2gisaid_id{$sample}){
		$duplicate_sample2gisaid_id{$sample} = $F[1];
		next;
	}
	$sample2gisaid_id{$sample} = $F[1];
}

# Parse the Zotero RDF export data for the bibliography 
my %sample2url;
my %url2title;
my %sample_skipped;
for my $uri (@ARGV){
	my $rdf = `cat $uri`;
	my $parser = RDF::Simple::Parser->new(base => $uri);
	my @triples = $parser->parse_rdf($rdf);
	for my $triple (@triples){
		my ($subject, $predicate, $object) = @$triple;
		if($predicate =~ /title/){ # typically Dublin Core
			#warn "Debug title: ($subject, $predicate, $object)\n";
			if(not exists $url2title{$subject}){ 
				$url2title{$subject} = $object;
			}
			elsif($url2title{$subject} ne $object){
				warn "Different titles for $subject, keeping original '", $url2title{$subject}, "', and ignoring redefinition '$object'\n";
			}
			# else ignoring as redundant
		}
		if($predicate =~ /description/){ # typically Dublin Core, encoding the "Extra" field from Zotero
			# Parse the Case ID field so the URL mapping can be applied to all cases in it (new line record separator since some IDs have space in them like "New Zealand/...")
			for my $sample (split /\n/s, $object){
				$sample =~ s/^\s+//; # remove any leading or trailing spaces
				$sample =~ s/\s+$//;
				if(not exists $sample2gisaid_id{$sample}){
					warn "Skipping output for '$sample' within article '$url2title{$subject}' because it was not defined in the GISAID metadata file $gisaid_metadata_file, either a typo or metadata needs updating.\n";
					$sample_skipped{$sample} = 1;
					next;
				}
				if(not exists $sample2url{$sample}){
					$sample2url{$sample} = $subject;
				}
				if(exists $superceding_sample2url{$sample}){
					$sample2url{$sample} = $superceding_sample2url{$sample};
				}
				# else keep the first one defined (RDF should be in temporal order, so the early bird gets the worm if not in the overrides file)
				if(exists $duplicate_sample2gisaid_id{$sample}){
					warn "Duplicate virus name '$sample', keeping first GISAID mapping '", $sample2gisaid_id{$sample}, "' and ignoring redefinition '$duplicate_sample2gisaid_id{$sample}' in $gisaid_metadata_file\n"; 
				}
			}
		}
	}
}

binmode STDOUT, ":encoding(UTF-8)"; # get rid of wide character printing warnings
for my $sample (sort keys %sample2url){
	my $url = $sample2url{$sample};
	if(not exists $url2title{$url}){
		warn "No title found in the RDF data for URL '$url', skipping\n";
		next;
	}
	print "$sample\t$sample2gisaid_id{$sample}\ttitle\t\"$url2title{$url}\"\n";
	print "$sample\t$sample2gisaid_id{$sample}\tpaper_url\t$url\n";
}

# There is a limit in Zotero on the size of an "extra" annotation, so we need to carefully add very long lists of IDs close to primry sources, like the UK COG Consortium phylodynamic analysis with 16K entries, to the superceding file
# and print them out even if the URL in the bibliography did not have all these IDs associated with it due to that Zotero limitation.
my %missing_title_reported;
for my $sample (keys %superceding_sample2url){
	next if exists $sample2url{$sample}; # we've already printed it
	next if exists $sample_skipped{$sample}; # we've already warned about skipping this one
	my $url = $superceding_sample2url{$sample};
	if(not exists $url2title{$url}){
		if(not exists $missing_title_reported{$url}){
			warn "!! Missing title in bibliography for superceding file entry URL '$url', skipping output for virus name '$sample' and any others superceding entries using that URL\n";
			$missing_title_reported{$url} = 1;
		}
		next;
	}
	if(not exists $sample2gisaid_id{$sample}){
		warn "Skipping output for '$sample' in the superceding file ($override_file) because it was not defined in the GISAID metadata file $gisaid_metadata_file, either a typo or metadata needs updating.\n";
		next;
	}
	print "$sample\t$sample2gisaid_id{$sample}\ttitle\t\"",$url2title{$superceding_sample2url{$sample}},"\"\n";
	print "$sample\t$sample2gisaid_id{$sample}\tpaper_url\t$url\n";
}
