export interface AminoAcidProps {
  name: string;
  threeLetter: string;
  oneLetter: string;
  type: 'polar' | 'nonpolar' | 'acidic' | 'basic' | 'aromatic';
  weight: number;
  codons: string[];
  charge: number;
  hydrophobicity: number; // Kyte-Doolittle scale
  description: string;
}

export interface SimulationConfig {
  sequence: string;
  modelType: 'QSVM' | 'VQE' | 'QNN' | 'QKernel';
  qubits: number;
  ansatz: 'RY/RZ + CNOT' | 'Hardware Efficient (HEA)' | 'QAOA';
  optimizer: 'SPSA' | 'COBYLA' | 'Adam';
  hostOrganism: 'Escherichia coli' | 'Homo sapiens' | 'Saccharomyces cerevisiae';
}

export interface SimulationResult {
  energyConvergence: number[];
  quantumCircuit: string;
  foldingStability: number; // percentage (0 - 100)
  deltaG: number; // kcal/mol
  bindingAffinity: number; // nM
  disulfideProb: number; // 0 - 1
  codonOptimized: string;
  codonGCContent: number;
  scientificReport: string;
  timestamp: string;
}

export const AMINO_ACIDS: Record<string, AminoAcidProps> = {
  A: {
    name: 'Alanine',
    threeLetter: 'Ala',
    oneLetter: 'A',
    type: 'nonpolar',
    weight: 89.1,
    codons: ['GCU', 'GCC', 'GCA', 'GCG'],
    charge: 0,
    hydrophobicity: 1.8,
    description: 'Small, hydrophobic, commonly found in alpha-helices with a simple methyl sidechain.'
  },
  R: {
    name: 'Arginine',
    threeLetter: 'Arg',
    oneLetter: 'R',
    type: 'basic',
    weight: 174.2,
    codons: ['CGU', 'CGC', 'CGA', 'CGG', 'AGA', 'AGG'],
    charge: 1,
    hydrophobicity: -4.5,
    description: 'Highly basic, positively charged amino acid with a flexible, proton-accepting guanidinium group.'
  },
  N: {
    name: 'Asparagine',
    threeLetter: 'Asn',
    oneLetter: 'N',
    type: 'polar',
    weight: 132.1,
    codons: ['AAU', 'AAC'],
    charge: 0,
    hydrophobicity: -3.5,
    description: 'Polar, uncharged amino acid featuring a carboxamide sidechain, often a site for N-linked glycosylation.'
  },
  D: {
    name: 'Aspartic Acid',
    threeLetter: 'Asp',
    oneLetter: 'D',
    type: 'acidic',
    weight: 133.1,
    codons: ['GAU', 'GAC'],
    charge: -1,
    hydrophobicity: -3.5,
    description: 'Acidic, negatively charged sidechain containing a carboxylate group, actively participates in salt bridges.'
  },
  C: {
    name: 'Cysteine',
    threeLetter: 'Cys',
    oneLetter: 'C',
    type: 'polar',
    weight: 121.2,
    codons: ['UGU', 'UGC'],
    charge: 0,
    hydrophobicity: 2.5,
    description: 'Thiol-bearing residue, critical for folding because it can form covalent disulfide bonds (bridges).'
  },
  E: {
    name: 'Glutamic Acid',
    threeLetter: 'Glu',
    oneLetter: 'E',
    type: 'acidic',
    weight: 147.1,
    codons: ['GAA', 'GAG'],
    charge: -1,
    hydrophobicity: -3.5,
    description: 'Negatively charged, acidic sidechain, highly hydrophilic, typically placed on outer protein surfaces.'
  },
  Q: {
    name: 'Glutamine',
    threeLetter: 'Gln',
    oneLetter: 'Q',
    type: 'polar',
    weight: 146.2,
    codons: ['CAA', 'CAG'],
    charge: 0,
    hydrophobicity: -3.5,
    description: 'Amide derivative of glutamic acid, very polar, participates heavily in hydrogen bonding networks.'
  },
  G: {
    name: 'Glycine',
    threeLetter: 'Gly',
    oneLetter: 'G',
    type: 'nonpolar',
    weight: 75.1,
    codons: ['GGU', 'GGC', 'GGA', 'GGG'],
    charge: 0,
    hydrophobicity: -0.4,
    description: 'The smallest amino acid with just a hydrogen atom sidechain, yielding maximum conformational flexibility.'
  },
  H: {
    name: 'Histidine',
    threeLetter: 'His',
    oneLetter: 'H',
    type: 'basic',
    weight: 155.2,
    codons: ['CAU', 'CAC'],
    charge: 0.1,
    hydrophobicity: -3.2,
    description: 'Contains an imidazole ring that functions as both a proton donor and acceptor in catalytic active sites.'
  },
  I: {
    name: 'Isoleucine',
    threeLetter: 'Ile',
    oneLetter: 'I',
    type: 'nonpolar',
    weight: 131.2,
    codons: ['AUU', 'AUC', 'AUA'],
    charge: 0,
    hydrophobicity: 4.5,
    description: 'Aliphatic, highly hydrophobic branch-chain amino acid, integral to forming dense hydrophobic cores.'
  },
  L: {
    name: 'Leucine',
    threeLetter: 'Leu',
    oneLetter: 'L',
    type: 'nonpolar',
    weight: 131.2,
    codons: ['UUA', 'UUG', 'CUU', 'CUC', 'CUA', 'CUG'],
    charge: 0,
    hydrophobicity: 3.8,
    description: 'Highly hydrophobic, promotes alpha-helix stabilization in biological trans-membrane helices.'
  },
  K: {
    name: 'Lysine',
    threeLetter: 'Lys',
    oneLetter: 'K',
    type: 'basic',
    weight: 146.2,
    codons: ['AAA', 'AAG'],
    charge: 1,
    hydrophobicity: -3.9,
    description: 'Positively charged basic amino acid with a primary amino group at the end of a flexible butyl chain.'
  },
  M: {
    name: 'Methionine',
    threeLetter: 'Met',
    oneLetter: 'M',
    type: 'nonpolar',
    weight: 149.2,
    codons: ['AUG'],
    charge: 0,
    hydrophobicity: 1.9,
    description: 'Sulfur-containing hydrophobic residue, coded exclusively by the universal translational start codon AUG.'
  },
  F: {
    name: 'Phenylalanine',
    threeLetter: 'Phe',
    oneLetter: 'F',
    type: 'aromatic',
    weight: 165.2,
    codons: ['UUU', 'UUC'],
    charge: 0,
    hydrophobicity: 2.8,
    description: 'Bulky aromatic sidechain, very hydrophobic, contributes to stacking interactions within protein cores.'
  },
  Pro: {
    name: 'Proline',
    threeLetter: 'Pro',
    oneLetter: 'P',
    type: 'nonpolar',
    weight: 115.1,
    codons: ['CCU', 'CCC', 'CCA', 'CCG'],
    charge: 0,
    hydrophobicity: -1.6,
    description: 'Cyclic imino acid sidechain that restricts mechanical mobility, causing structural loops or helix kinks.'
  },
  S: {
    name: 'Serine',
    threeLetter: 'Ser',
    oneLetter: 'S',
    type: 'polar',
    weight: 105.1,
    codons: ['UCU', 'UCC', 'UCA', 'UCG', 'AGU', 'AGC'],
    charge: 0,
    hydrophobicity: -0.8,
    description: 'Hydroxyl-bearing residue, highly polar, participates in enzymatic catalysis and phosphorylation reactions.'
  },
  T: {
    name: 'Threonine',
    threeLetter: 'Thr',
    oneLetter: 'T',
    type: 'polar',
    weight: 119.1,
    codons: ['ACU', 'ACC', 'ACA', 'ACG'],
    charge: 0,
    hydrophobicity: -0.7,
    description: 'Contains a polar secondary alcohol group and is frequently modified via O-linked glycosylation.'
  },
  W: {
    name: 'Tryptophan',
    threeLetter: 'Trp',
    oneLetter: 'W',
    type: 'aromatic',
    weight: 204.2,
    codons: ['UGG'],
    charge: 0,
    hydrophobicity: -0.9,
    description: 'The largest standard amino acid, indole aromatic ring sidechain, emits intrinsic fluorescence.'
  },
  Y: {
    name: 'Tyrosine',
    threeLetter: 'Tyr',
    oneLetter: 'Y',
    type: 'aromatic',
    weight: 181.2,
    codons: ['UAU', 'UAC'],
    charge: 0,
    hydrophobicity: -1.3,
    description: 'Aromatic with a phenolic hydroxyl group, amphipathic (both polar and non-polar characteristics).'
  },
  V: {
    name: 'Valine',
    threeLetter: 'Val',
    oneLetter: 'V',
    type: 'nonpolar',
    weight: 117.1,
    codons: ['GUU', 'GUC', 'GUA', 'GUG'],
    charge: 0,
    hydrophobicity: 4.2,
    description: 'Aliphatic hydrophobic branched amino acid, essential for mechanical core packing.'
  },
  P: {
    name: 'Proline',
    threeLetter: 'Pro',
    oneLetter: 'P',
    type: 'nonpolar',
    weight: 115.1,
    codons: ['CCU', 'CCC', 'CCA', 'CCG'],
    charge: 0,
    hydrophobicity: -1.6,
    description: 'Cyclic amino acid structurally restricting protein backbone torsion angles to introduce loops/kinks.'
  }
};

export const PRESETS = [
  {
    name: 'Human Insulin (Chain A)',
    sequence: 'GIVEQCCTSICSLYQLENYCN',
    description: 'Controls glucose homeostasis. Features critical cysteine bonds.'
  },
  {
    name: 'Green Fluorescent Protein (Chromophore Region)',
    sequence: 'FSYGVQCFSRYP',
    description: 'Jellyfish folding center which self-catalyzes the green fluorophore.'
  },
  {
    name: 'SARS-CoV-2 Spike Receptor Domain Mutant',
    sequence: 'YGFYTTTGIGYQPYRVVVLSFELL',
    description: 'Interacts with human ACE2 receptor. Studied for quantum docking affinity.'
  },
  {
    name: 'Ubiquitin (N-terminal Beta-hairpin)',
    sequence: 'MQIFVKTLTGKTITLEVEPSD',
    description: 'Highly stable fold, tag for proteasomal degradation pathways.'
  }
];