#!/bin/bash

echo '> Install variants app from CGS'
sudo python installCGSapps.py variants

echo '> Install modules for the variants app'
sudo easy_install pip
sudo pip install ordereddict
sudo pip install counter
#sudo yum install -y git
# git clone https://github.com/jamescasbon/PyVCF
# sudo python PyVCF/setup.py install
sudo pip install pyvcf
sudo pip install djangorestframework==3.2.5
sudo pip install markdown
sudo pip install django-filter

#echo '> Modify some configuration'
#sudo cp hive-site.xml /etc/hive/conf.dist/hive-site.xml
#sudo /usr/lib/spark/sbin/stop-all.sh
#sudo /usr/lib/spark/sbin/start-all.sh

echo '> Put Highlander data in hdfs'
# hdfs dfs -put highlander-data /user/cloudera/highlander-data

echo '> Import Highlander data in MySQL database'
mysql -u root -p root highlander < highlander.sql

echo '> Import Highlander data in Impala database'
sudo impala-shell --query='create table default.highlander_variant_txt (id int, platform string, outsourcing string, project_id int, run_label string, patient string, pathology string, `partition` int, sample_type string, chr string, pos int, reference string, alternative string, change_type string, hgvs_protein string, hgvs_dna string, gene_symbol string, exon_intron_rank int, exon_intron_total int, cdna_pos int, cdna_length int, cds_pos int, cds_length int, protein_pos int, protein_length int, gene_ensembl string, num_genes int, biotype string, transcript_ensembl string, transcript_uniprot_id string, transcript_uniprot_acc string, transcript_refseq_prot string, transcript_refseq_mrna string, dbsnp_id_137 string, dbsnp_id_141 string, unisnp_ids string, clinvar_rs string, clinvar_clnsig string, clinvar_trait string, cosmic_id string, cosmic_count int, filters string, confidence double, variant_confidence_by_depth double, fisher_strand_bias double, mapping_quality double, haplotype_score double, rank_sum_test_read_mapping_qual double, rank_sum_test_read_pos_bias double, rank_sum_test_base_qual double, read_depth int, mapping_quality_zero_reads int, downsampled int, allele_num int, allelic_depth_ref int, allelic_depth_alt int, allelic_depth_proportion_ref double, allelic_depth_proportion_alt double, mle_allele_count int, mle_allele_frequency double, allelic_unique_starts_ref int, allelic_unique_starts_alt int, allelic_qv_ref int, allelic_qv_alt int, zygosity string, genotype_quality double, genotype_likelihood_hom_ref int, genotype_likelihood_het int, genotype_likelihood_hom_alt int, snpeff_effect string, snpeff_impact string, sift_score double, sift_pred string, pph2_hdiv_score double, pph2_hdiv_pred string, pph2_hvar_score double, pph2_hvar_pred string, lrt_score double, lrt_pred string, mutation_taster_score double, mutation_taster_pred string, mutation_assessor_score double, mutation_assessor_pred string, fathmm_score double, fathmm_pred string, aggregation_score_radial_svm double, aggregation_pred_radial_svm string, aggregation_score_lr double, aggregation_pred_lr string, reliability_index int, vest_score double, cadd_raw double, cadd_phred double, is_scsnv_refseq int, is_scsnv_ensembl int, splicing_ada_score double, splicing_ada_pred string, splicing_rf_score double, splicing_rf_pred string, consensus_prediction int, other_effects int, gerp_nr double, gerp_rs double, phylop46way_primate double, phylop46way_placental double, phylop100way_vertebrate double, phastcons46way_primate double, phastcons46way_placental double, phastcons100way_vertebrate double, siphy_29way_pi string, siphy_29way_log_odds double, exac_ac int, exac_af double, 1000g_ac int, 1000g_af double, esp6500_aa_af double, esp6500_ea_af double, gonl_ac int, gonl_af double, aric5606_aa_ac int, aric5606_aa_af double, aric5606_ea_ac int, aric5606_ea_af double, consensus_mac int, consensus_maf double, lof_tolerant_or_recessive_gene string, short_tandem_repeat int, repeat_unit string, repeat_number_ref int, repeat_number_alt int, evaluation int, evaluation_username string, check_insilico int, check_insilico_username string, check_validated_change int, check_validated_change_username string, check_somatic_change int, check_somatic_change_username string, check_segregation string, check_segregation_username string, evaluation_comments string, history string, public_comments string, insert_date string) row format delimited terminated by "|" stored as textfile;'

impala-shell --query='create table default.highlander_variant like default.highlander_variant_txt stored as parquet;'

impala-shell --query='load data inpath "/user/cloudera/highlander-data/highlander.tsv" overwrite into table default.highlander_variant_txt'

impala-shell --query='insert into table default.highlander_variant select * from default.highlander_variant_txt';


echo '> Install database by going on: http://quickstart.cloudera:8888/variants/database/initialize'

