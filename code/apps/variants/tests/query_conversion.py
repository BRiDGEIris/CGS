
raw_queries = ['select outsourcing, run_label, pathology from 1000g where project_id = 1 order by id',]

def get_mapping():
    map_highlander = ['id','platform','outsourcing','project_id','run_label','patient','pathology','partition','sample_type','chr','pos','reference','alternative','change_type','cdna_length','gene_symbol','gene_ensembl','num_genes','biotype','transcript_ensembl','transcript_uniprot_id','transcript_uniprot_acc','transcript_refseq_prot','transcript_refseq_mrna','dbsnp_id_137','unisnp_ids','exon_intron_total','filters','confidence','variant_confidence_by_depth','largest_homopolymer','strand_bias','read_depth','mapping_quality_zero_reads','downsampled','allele_num','allelic_depth_ref','allelic_depth_alt','allelic_depth_proportion_ref','allelic_depth_proportion_alt','allelic_unique_starts_ref','allelic_unique_starts_alt','allelic_qv_ref','allelic_qv_alt','zygosity','genotype_quality','genotype_likelihood_hom_ref','genotype_likelihood_het','genotype_likelihood_hom_alt','snpeff_effect','snpeff_impact','sift_score','sift_pred','pph2_hdiv_score','pph2_hdiv_pred','pph2_hvar_score','pph2_hvar_pred','lrt_score','lrt_pred','mutation_taster_score','mutation_taster_pred','mutation_assessor_score','mutation_assessor_pred','consensus_prediction','other_effects','gerp_nr','gerp_rs','siphy_29way_pi','siphy_29way_log_odds','1000G_AC','1000G_AF','ESP6500_AA_AF','ESP6500_EA_AF','lof_tolerant_or_recessive_gene','rank_sum_test_base_qual','rank_sum_test_read_mapping_qual','rank_sum_test_read_pos_bias','haplotype_score','found_in_exomes_haplotype_caller','found_in_exomes_lifescope','found_in_genomes_haplotype_caller','found_in_panels_haplotype_caller','check_insilico','check_insilico_username','check_validated_change','check_validated_change_username','check_somatic_change','check_somatic_change_username','public_comments','insert_date','fisher_strand_bias','mapping_quality','mle_allele_count','mle_allele_frequency','short_tandem_repeat','repeat_unit','repeat_number_ref','repeat_number_alt','fathmm_score','fathmm_pred','aggregation_score_radial_svm','aggregation_pred_radial_svm','aggregation_score_lr','aggregation_pred_lr','reliability_index','gonl_ac','gonl_af','found_in_crap','hgvs_dna','hgvs_protein','cds_length','cds_pos','cdna_pos','exon_intron_rank','phyloP46way_primate','evaluation','evaluation_username','evaluation_comments','history','check_segregation','check_segregation_username','found_in_panels_torrent_caller','cosmic_id','cosmic_count','is_scSNV_RefSeq','is_scSNV_Ensembl','splicing_ada_score','ARIC5606_EA_AC','ARIC5606_EA_AF','clinvar_rs','clinvar_clnsig','clinvar_trait','phastCons46way_primate','phastCons46way_placental','phastCons100way_vertebrate','ARIC5606_AA_AC','ARIC5606_AA_AF','phyloP100way_vertebrate','phyloP46way_placental','cadd_phred','cadd_raw','vest_score','dbsnp_id_141','splicing_ada_pred','splicing_rf_score','splicing_rf_pred','protein_pos','protein_length']
    map_impala = ['R.ID','','','R.SI','','','','/','','R.C','R.P','R.REF','R.ALT','I.CT','I.CDNAL','R.GS','R.GIE','I.NG','I.BIOT','I.TRE','I.TRUID','I.TRUAC','I.TRRP','I.TRRM','I.DBSNP137','I.UNID','I.EIT','R.FILTER','R.QUAL','I.QD','I.HR','I.SB','F.DPF','I.MQ0','I.DS','I.AN','F.ADREF','F.ADALT','F.ADPR','F.ADPA','/','/','/','/','F.ZYG','F.GQ','F.GLHR','F.GLH','F.GLHA','I.SNPE','I.SNPI','I.SIFTS','I.SIFTP','I.PHS','I.PHP','I.PVS','I.PVP','I.LRTS','I.LRTP','I.MTS','I.MTP','I.MAS','I.MAP','I.CP','I.ANN','I.GENR','I.GERS','I.S2PI','I.S2LO','I.AC1000G','I.AF1000G','I.EAAF','I.EEAF','I.LOF','I.RBQ','I.RRMQ','I.RPB','I.HS','I.FEHC','I.FEL','I.FGHC','I.FPHC','I.CI','I.CIU','I.CVC','I.CVCU','I.CSC','I.CSCU','I.PC','I.ID','I.FS','I.MQ','I.MLC','I.MLF','I.STR','I.RU','I.RPAR','I.RPAA','I.FAS','I.FAP','I.ASRS','I.APRS','I.ASL','I.APL','I.RIN','I.GONLAC','I.GONLAF','I.FIC','I.HGD','I.HGP','I.CDL','I.CDP','I.CDAP','I.EXIR','I.PHPR','I.EV','I.EVU','I.EVC','I.HIST','I.CS','I.CSU','I.FPTC','I.COID','I.COCO','I.ISREF','I.ISEN','I.SPAS','I.AEAC','I.AEAF','I.CLRS','I.CLCL','I.CLTR','I.PHAPR','I.PHAPL','I.PHAV','I.ARAC','I.ARAF','I.PHYV','I.PHYP','I.CAPH','I.CARA','I.VES','I.DB141','I.SPAP','I.SPRS','I.SPRP','I.PROPO','I.PROLE']
    
    mapping = {}
    for i in xrange(0, len(map_highlander)):
        mapping[map_highlander[i]] = map_impala[i].lower().replace('.','_')
    return mapping

def transformation(raw_query):
    """
        For now we don't care about the 'limit', 'order by' and 'group by' or other stuff like that
    """
    # Mapping table between a highlander query on MySQL and the Impala table from CGS    
    mapping = get_mapping()
    
    # We directly map the fields we have (only the 'basic' ones)
    for map in mapping:
        if len(mapping[map]) == 0 or mapping[map] == '/':
            # No mapping available, we would need to remove the field later
            pass
        else:
            raw_query = raw_query.replace(map,mapping[map])
    
    # We split the raw_query to be able to construct our impala query at the end 
    tmp = raw_query.split(' where ')
    select = tmp[0] # We suppose we have a 'select' in the query
    select = 'select * ' # To avoid some problems during the formatting of the results, we select every field just in case
    if len(tmp) > 1:
        pos = -1
        first_condition = ''
        for condition in ['order by','group by','limit']:
            tmp_pos = tmp[1].find(condition)
            if (tmp_pos < pos or pos == -1) and tmp_pos != -1:
                pos = tmp_pos
                first_condition = condition
                
        if pos == -1:
            where = tmp[1]
        else:
            print('first condition: '+first_condition+' ('+str(pos)+')')
            where = tmp[1].split(first_condition)[0]
            restriction = tmp[1][pos:]
    else:
        # There is no 'where' condition
        where = ''
    # Now we create the query for impala
    impala_where = ''
    where_fields = where.split(',')
    dest_fields = []
    for where_field in where_fields:
        current_where = where_field.strip()
        
        
        dest_fields.append(current_where)
    where = 'where '+','.join(dest_fields) 
    return select+' '+where+' '+restriction
        
for raw_query in raw_queries:
    result = transformation(raw_query)
    print(raw_query+' > '+result)
    
    