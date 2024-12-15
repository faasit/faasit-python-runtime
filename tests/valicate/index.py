from faasit_runtime import create_handler, workflow, Workflow

@workflow
def mlpipeline(wf: Workflow):
    s0 = wf.call('stage0',{
        'input': 'Digits_Train.txt',
        'output': {
            'vectors_pca': 'vectors_pca',
            'train_pca_transform': 'train_pca_transform',
        }
    })
    s1 = wf.call('stage1',{'stage0':s0, 
        'input': {
            'train_pca_transform': 'train_pca_transform'
        },
        'output': {
            'model': 'model_tree_0_0'
        }
    })
    s2 = wf.call('stage2',{'stage1':s1, 'stage0':s0, 
        'input': {
            'train_pca_transform': 'train_pca_transform',
            'model': 'model_tree_0_0'
        },
        'output': {
            'predict': 'predict_0',
        }
    })
    s3 = wf.call('stage3',{'stage2':s2, 'stage0':s0,
        'input': {
            'train_pca_transform': 'train_pca_transform',
            'predict': 'predict_0',
        },
    })

    return s3

handler = create_handler(mlpipeline)